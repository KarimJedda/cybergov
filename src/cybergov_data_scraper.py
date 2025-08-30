import json
from typing import Dict, Any, Optional

import httpx
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret, String
from prefect.tasks import exponential_backoff
import s3fs
import datetime
from prefect.server.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
    DeploymentFilter,
    DeploymentFilterId,
    FlowRunFilterName,
)
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import StateType
from utils.constants import (
    NETWORK_MAP,
    INFERENCE_SCHEDULE_DELAY_MINUTES,
    INFERENCE_TRIGGER_DEPLOYMENT_ID,
)


class ProposalFetchError(Exception):
    pass


class ProposalParseError(Exception):
    pass


@task(
    name="Fetch Proposal JSON from Subsquare API",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.2,
)
def fetch_subsquare_proposal_data(url: str) -> Dict[str, Any]:
    """
    Fetches and parses proposal data from a Subsquare JSON API endpoint.
    """
    logger = get_run_logger()
    user_agent_secret = Secret.load("cybergov-scraper-user-agent")
    user_agent = user_agent_secret.get()

    headers = {"User-Agent": user_agent, "Accept": "application/json"}

    logger.info(f"Fetching JSON data from API: {url}")

    try:
        with httpx.Client() as client:
            response = client.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched and parsed JSON from {url}")
            return data

    except httpx.RequestError as e:
        logger.error(f"HTTP Request failed for URL {url}: {e}")
        raise ProposalFetchError(f"Failed to fetch data from {url}") from e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response from {url}: {e}")
        raise ProposalParseError(f"API response from {url} was not valid JSON.") from e


@task(name="Save JSON to S3", retries=2, retry_delay_seconds=5)
def save_to_s3(
    data: Dict[str, Any],
    s3_bucket: str,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    full_s3_path: str,
):
    """Saves the extracted JSON data to a specified S3 path."""
    logger = get_run_logger()
    logger.info(f"Saving JSON to {full_s3_path}...")
    try:
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint_url,
            },
        )

        with s3.open(full_s3_path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"✅ Success! Proposal data saved to {full_s3_path}")
    except Exception as e:
        logger.error(f"❌ Failed to write to S3 at {full_s3_path}: {e}")
        raise


@flow(name="Fetch and Store Raw Subsquare Data")
def fetch_and_store_raw_subsquare_data(network: str, proposal_id: int) -> Optional[str]:
    """
    Subflow to handle fetching, parsing, and storing raw data for one proposal.
    Returns the S3 path of the stored data, or None if skipped.
    """

    s3_bucket_block = String.load("scaleway-bucket-name")
    endpoint_block = String.load("scaleway-s3-endpoint-url")
    access_key_block = Secret.load("scaleway-write-access-key-id")
    secret_key_block = Secret.load("scaleway-write-secret-access-key")

    s3_bucket = s3_bucket_block.value
    endpoint_url = endpoint_block.value
    access_key = access_key_block.get()
    secret_key = secret_key_block.get()

    base_url = NETWORK_MAP[network]
    proposal_url = f"{base_url}/{proposal_id}"
    s3_output_path = (
        f"{s3_bucket}/proposals/{network}/{proposal_id}/raw_subsquare_data.json"
    )

    proposal_data = fetch_subsquare_proposal_data(proposal_url)

    save_to_s3(
        data=proposal_data,
        s3_bucket=s3_bucket,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        full_s3_path=s3_output_path,
    )

    return s3_output_path


@task
async def check_if_already_scheduled(proposal_id: int, network: str) -> bool:
    """
    Checks the Prefect API to see if a scraper run for this proposal
    already exists (in a non-failed state).
    """
    logger = get_run_logger()
    logger.info(
        f"Checking for existing flow runs for inference-{network}-{proposal_id}..."
    )

    async with get_client() as client:
        existing_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                name=FlowRunFilterName(like_=f"inference-{network}-{proposal_id}"),
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(
                        any_=[
                            StateType.RUNNING,
                            StateType.COMPLETED,
                            StateType.PENDING,
                            StateType.SCHEDULED,
                        ]
                    )
                ),
            ),
            deployment_filter=DeploymentFilter(
                id=DeploymentFilterId(any_=[INFERENCE_TRIGGER_DEPLOYMENT_ID])
            ),
        )

    if existing_runs:
        logger.warning(
            f"Found {len(existing_runs)} existing inference run(s) for proposal {proposal_id} on '{network}'. Skipping scheduling."
        )
        return True

    logger.info(
        f"No existing inference runs found for proposal {proposal_id} on '{network}'. It's safe to schedule."
    )
    return False


@task
async def schedule_inference_task(proposal_id: int, network: str):
    """Schedules the cybergov_scraper flow to run in the future."""
    logger = get_run_logger()

    delay = datetime.timedelta(minutes=INFERENCE_SCHEDULE_DELAY_MINUTES)
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + delay
    logger.info(
        f"Scheduling MAGI inference for proposal {proposal_id} on '{network}' "
        f"to run at {scheduled_time.isoformat()}"
    )

    async with get_client() as client:
        await client.create_flow_run_from_deployment(
            name=f"inference-{network}-{proposal_id}",
            deployment_id=INFERENCE_TRIGGER_DEPLOYMENT_ID,
            parameters={"proposal_id": proposal_id, "network": network},
            # state=Scheduled(scheduled_time=scheduled_time)
        )


@task(name="Archive Previous Run Data")
def archive_previous_run(network: str, proposal_id: int):
    """
    Checks for existing data for a proposal. If found, archives it into a
    versioned 'vote_archive_{index}' subfolder before the new run proceeds.

    Because Governance is messy :)
    """
    logger = get_run_logger()

    s3_bucket_block = String.load("scaleway-bucket-name")
    endpoint_block = String.load("scaleway-s3-endpoint-url")
    access_key_block = Secret.load("scaleway-write-access-key-id")
    secret_key_block = Secret.load("scaleway-write-secret-access-key")

    s3_bucket = s3_bucket_block.value
    endpoint_url = endpoint_block.value
    access_key = access_key_block.get()
    secret_key = secret_key_block.get()

    base_path = f"{s3_bucket}/proposals/{network}/{proposal_id}"

    s3 = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={
            "endpoint_url": endpoint_url,
        },
    )

    # 1. Check if the base directory has any contents
    existing_contents = s3.ls(base_path, detail=False)
    logger.info(f"Found {len(existing_contents)} items in {base_path}.")
    if not existing_contents:
        logger.info(f"No previous data found at {base_path}. This is the first run.")
        return

    logger.info(f"Found {len(existing_contents)} items in {base_path}. Archiving...")

    # 2. Find the next available vote_archive_index
    vote_archive_index = 0
    while True:
        archive_path = f"{base_path}/vote_archive_{vote_archive_index}"
        if not s3.exists(archive_path):
            logger.info(f"Next available archive folder is: {archive_path}")
            break
        vote_archive_index += 1

    # 3. Identify items to move (everything except existing 'vote_archive_' folders)
    items_to_move = [item for item in existing_contents if not item.split('/')[-1].startswith('vote_archive_')]

    # 4. Move the identified items into the new archive folder
    if items_to_move:
        logger.info(f"Moving {len(items_to_move)} items to {archive_path}...")
        for source_path in items_to_move:
            # Extract the base name (file or folder name) from the source path
            base_name = source_path.split('/')[-1]
            destination_path = f"{archive_path}/{base_name}"
            logger.info(f"Moving {source_path} -> {destination_path}")
            s3.mv(source_path, destination_path, recursive=True)
        logger.info("✅ Move operation completed.")
    else:
        logger.info("No new items to archive (only found existing vote_archive_* folders).")


@task(name="Enrich data with on-chain infos and misc stuff")
def enrich_proposal_data(network: str, proposal_id: int):
    # takes the raw_data_path and then does stuff with it, grounding vector
    # ideas:
    # does the proposer have a registered identity?
    # how many proposals has this identity submitted?
    # etc
    pass 


@task(name="Enrich data with on-chain infos and misc stuff")
def generate_prompt_content(network: str, proposal_id: int):
    """
    Reads raw proposal data from S3, generates a markdown file with dummy content,
    and writes it back to S3.
    """
    logger = get_run_logger()
    logger.info(f"Starting content generation for {network} proposal {proposal_id}.")

    # TODO clean up all this S3 mess 
    s3_bucket_block = String.load("scaleway-bucket-name")
    endpoint_block = String.load("scaleway-s3-endpoint-url")
    access_key_block = Secret.load("scaleway-write-access-key-id")
    secret_key_block = Secret.load("scaleway-write-secret-access-key")

    s3_bucket = s3_bucket_block.value
    endpoint_url = endpoint_block.value
    access_key = access_key_block.get()
    secret_key = secret_key_block.get()


    input_s3_path = (
        f"{s3_bucket}/proposals/{network}/{proposal_id}/raw_subsquare_data.json"
    )
    output_s3_path = (
        f"{s3_bucket}/proposals/{network}/{proposal_id}/content.md"
    )
    logger.info(f"Reading from: {input_s3_path}")
    logger.info(f"Writing to: {output_s3_path}")


    try:
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint_url,
            },
        )

        logger.info(f"Reading source file {input_s3_path}...")
        with s3.open(input_s3_path, "r") as f:
            # Parse the raw subsquare data here
            input_data = json.load(f)
        logger.info("✅ Source data read successfully.")

        # TODO This should be built by an alternative LLM
        dummy_markdown_content = f"""
# Analysis for Proposal {proposal_id} on {network}

## Summary
This is a placeholder summary generated by the `generate_prompt_content` task.
The data was successfully read from `{input_s3_path}`.

## Details
- **Network**: {network.capitalize()}
- **Proposal ID**: {proposal_id}
- **Status**: Ready for LLM processing.

*This content is for demonstration purposes and will be replaced with real on-chain and off-chain data enrichment in the future.*
"""

        # Write the new content.md file
        logger.info(f"Writing dummy markdown to {output_s3_path}...")
        with s3.open(output_s3_path, "w") as f:
            f.write(dummy_markdown_content)
        
        logger.info(f"✅ Success! Prompt content saved to {output_s3_path}")

    except FileNotFoundError:
        logger.error(f"❌ Input file not found at {input_s3_path}. The previous task may have failed.")
        raise
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred during S3 operations: {e}")
        raise

@flow(name="Fetch Proposal Data")
async def fetch_proposal_data(network: str, proposal_id: int):
    """
    Fetch relevant data for a proposal, parse its data, and save it to S3.
    """
    logger = get_run_logger()

    if network not in NETWORK_MAP:
        logger.error(
            f"Invalid network '{network}'. Must be one of {list(NETWORK_MAP.keys())}"
        )
        return

    try:
        logger.info("Check if new run")
        archive_previous_run(
            network=network,
            proposal_id=proposal_id,
        )

        logger.info(f"Fetching data for proposal {proposal_id} on {network}")
        raw_data_s3_path = fetch_and_store_raw_subsquare_data(
            network=network,
            proposal_id=proposal_id,
        )

        logger.info(f"Raw data is available at: {raw_data_s3_path}")

        logger.info("Placeholder for enrichment tasks.")
        enrich_proposal_data(network=network, proposal_id=proposal_id)

        logger.info("Placeholder for LLM prompt generation.")
        generate_prompt_content(network=network, proposal_id=proposal_id)

        logger.info(
            "All good! Now scheduling the inference in 30 minutes. If inference successful, schedule vote & comment too!."
        )

        is_already_scheduled = await check_if_already_scheduled(
            proposal_id=proposal_id, network=network
        )
        if not is_already_scheduled:
            await schedule_inference_task(proposal_id=proposal_id, network=network)

    except (ProposalFetchError, ProposalParseError) as e:
        logger.error(f"Pipeline failed for {network} ref {proposal_id}. Reason: {e}")
        raise
    except Exception as e:
        logger.error(
            f"An unexpected error occurred in the pipeline for {network} ref {proposal_id}: {e}"
        )
        raise


if __name__ == "__main__":
    import asyncio
    asyncio.run(
        fetch_proposal_data(
            network="paseo", 
            proposal_id=100
        )
    )
