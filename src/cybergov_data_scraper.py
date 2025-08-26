import json
from typing import Dict, Any, Optional

from bs4 import BeautifulSoup
import httpx
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret, String
from prefect.tasks import exponential_backoff
import s3fs

INFERENCE_TRIGGER_DEPLOYMENT_ID = "089b702f-f2fc-4605-8f08-44d222727695" 

INFERENCE_SCHEDULE_DELAY_MINUTES = 30

NETWORK_MAP = {
    "polkadot": "https://polkadot-api.subsquare.io/gov2/referendums",
    "kusama": "https://kusama-api.subsquare.io/gov2/referendums",
    "paseo": "https://paseo-api.subsquare.io/gov2/referendums"
}

class ProposalFetchError(Exception):
    pass

class ProposalParseError(Exception):
    pass

@task(
    name="Fetch Proposal JSON from Subsquare API",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.2
)
def fetch_subsquare_proposal_data(url: str) -> Dict[str, Any]:
    """
    Fetches and parses proposal data from a Subsquare JSON API endpoint.
    """
    logger = get_run_logger()
    user_agent_secret = Secret.load("cybergov-scraper-user-agent")
    user_agent = user_agent_secret.get()
    
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json"
    }
    
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


@task(
    name="Save JSON to S3",
    retries=2,
    retry_delay_seconds=5
)
def save_to_s3(data: Dict[str, Any], s3_bucket: str, endpoint_url: str, access_key: str, secret_key: str, full_s3_path: str):
    """Saves the extracted JSON data to a specified S3 path."""
    logger = get_run_logger()
    logger.info(f"Saving JSON to {full_s3_path}...")
    try:
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint_url,
            }
        )

        with s3.open(full_s3_path, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"✅ Success! Proposal data saved to {full_s3_path}")
    except Exception as e:
        logger.error(f"❌ Failed to write to S3 at {full_s3_path}: {e}")
        raise


@flow(name="Fetch and Store Raw Subsquare Data")
def fetch_and_store_raw_subsquare_data(
    network: str, proposal_id: int
) -> Optional[str]:
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
    s3_output_path = f"{s3_bucket}/proposals/{network}/{proposal_id}/raw_subsquare_data.json"

    proposal_data = fetch_subsquare_proposal_data(proposal_url)
    
    save_to_s3(
        data=proposal_data,
        s3_bucket=s3_bucket,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        full_s3_path=s3_output_path
    )

    return s3_output_path


@task
async def check_if_already_scheduled(proposal_id: int, network: str) -> bool:
    """
    Checks the Prefect API to see if a scraper run for this proposal
    already exists (in a non-failed state).
    """
    logger = get_run_logger()
    logger.info(f"Checking for existing flow runs for inference-{network}-{proposal_id}...")

    async with get_client() as client:
        existing_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                name=FlowRunFilterName(like_=f"inference-{network}-{proposal_id}"),
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(
                        any_=[StateType.RUNNING, StateType.COMPLETED, StateType.PENDING, StateType.SCHEDULED]
                    )
                )
            ),
            deployment_filter=DeploymentFilter(
                id=DeploymentFilterId(any_=[INFERENCE_TRIGGER_DEPLOYMENT_ID])
            )
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
        )



@flow(name="Fetch Proposal Data")
def fetch_proposal_data(network: str, proposal_id: int):
    """
    Fetch relevant data for a proposal, parse its data, and save it to S3.
    """
    logger = get_run_logger()

    if network not in NETWORK_MAP:
        logger.error(f"Invalid network '{network}'. Must be one of {list(NETWORK_MAP.keys())}")
        return

    try:
        raw_data_s3_path = fetch_and_store_raw_subsquare_data(
            network=network,
            proposal_id=proposal_id,
        )

        logger.info(f"Raw data is available at: {raw_data_s3_path}")

        logger.info("Placeholder for enrichment tasks.")

        logger.info("Placeholder for LLM prompt generation.")

        logger.info("All good! Now scheduling the inference in 30 minutes. If inference successful, schedule vote & comment too!.")

        is_already_scheduled = await check_if_already_scheduled(
                proposal_id=proposal_id, 
                network=network
            )
        if not is_already_scheduled:
            schedule_inference_task(proposal_id=proposal_id, network=network)

    except (ProposalFetchError, ProposalParseError) as e:
        logger.error(f"Pipeline failed for {network} ref {proposal_id}. Reason: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred in the pipeline for {network} ref {proposal_id}: {e}")
        raise