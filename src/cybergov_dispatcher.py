import datetime
from typing import List, Optional, Dict
from prefect import flow, task, get_run_logger
import s3fs
import httpx
import os
from prefect.blocks.system import String, Secret
from prefect.server.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
    DeploymentFilter,
    DeploymentFilterId,
    FlowRunFilterName,
)
from prefect.client.orchestration import get_client
from prefect.states import Scheduled
from prefect.client.schemas.objects import StateType
from utils.constants import (
    SCRAPING_SCHEDULE_DELAY_DAYS,
    DATA_SCRAPER_DEPLOYMENT_ID,
    CYBERGOV_PARAMS,
)


@task
async def get_last_processed_id_from_s3(
    network: str, s3_bucket: str, access_key: str, secret_key: str, endpoint_url: str
) -> int:
    """
    Checks S3-compatible storage (like Scaleway) using credentials and a specific endpoint_url.
    """
    logger = get_run_logger()
    s3_path = f"{s3_bucket}/proposals/{network}/"
    logger.info(
        f"Checking for existing proposals in S3-compatible storage path: s3://{s3_path}"
    )
    logger.info(f"Using S3 endpoint: {endpoint_url}")

    try:
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint_url,
            },
        )

        existing_proposal_paths = s3.ls(s3_path, detail=False)

        proposal_ids = []
        for path in existing_proposal_paths:
            last_component = os.path.basename(path.rstrip("/"))
            if last_component.isdigit():
                proposal_ids.append(int(last_component))

        if not proposal_ids:
            logger.info(
                f"No existing proposals found for '{network}'. Starting from 0."
            )
            return 0

        max_id = max(proposal_ids)
        logger.info(f"Highest proposal ID found for '{network}' is {max_id}.")
        return max_id

    except FileNotFoundError:
        logger.info(
            f"S3 path s3://{s3_path} does not exist. Assuming first run for '{network}'."
        )
        return 0
    except Exception as e:
        logger.error(f"Failed to list S3 directory 's3://{s3_path}': {e}")
        raise


@task
async def find_new_proposals(network: str, last_known_id: int) -> List[Dict]:
    """Fetching proposals from Subsquare with an index > last_known_id."""
    logger = get_run_logger()
    logger.info(
        f"Checking for new proposals on '{network}' after ID {last_known_id}..."
    )

    try:
        # Using sidacar, am lazy, also this will allow to automatically be ready for the migration. nice
        network_sidecar_block = await Secret.load(f"{network}-sidecar-url")
        network_sidecar_url = network_sidecar_block.get()
    except Exception as e:
        logger.error(f"Failed to load secret for network '{network}': {e}")
        raise

    url = f"{network_sidecar_url}/pallets/referenda/storage/referendumCount"

    headers = {"Accept": "application/json"}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, timeout=15.0)
            response.raise_for_status()

            data = response.json()
            last_proposal_id = data.get("value", None)

            if last_proposal_id is not None:
                last_proposal_id = int(last_proposal_id)
                logger.info(
                    f"Successfully fetched last proposal ID for '{network}': {last_proposal_id}"
                )
            else:
                logger.warning(f"No 'value' found in JSON response: {data}")
                raise

    except httpx.RequestError as e:
        logger.error(f"HTTP request failed for {url}: {e}")
        raise
    except httpx.HTTPStatusError as e:
        logger.error(f"Non-200 response: {e.response.status_code} - {e.response.text}")
        raise
    except ValueError as e:
        logger.error(f"Response is not valid JSON: {e} - Body: {response.text}")
        raise

    min_threshold = CYBERGOV_PARAMS.get("min_proposal_id", {}).get(network, 0)
    start_from_id = max(last_known_id, min_threshold)

    if last_proposal_id < start_from_id:
        logger.info(
            f"Proposal ID {last_proposal_id} is below network's min proposal threshold ({min_threshold}), skipping."
        )
        return []

    new_proposals = [
        {"proposalIndex": i} for i in range(start_from_id + 1, last_proposal_id)
    ]
    logger.info(f"Found {len(new_proposals)} new proposals for '{network}'.")
    return new_proposals


@task
async def check_if_already_scheduled(proposal_id: int, network: str) -> bool:
    """
    Checks the Prefect API to see if a scraper run for this proposal
    already exists (in a non-failed state).
    """
    logger = get_run_logger()
    logger.info(f"Checking for existing flow runs for {network}-{proposal_id}...")

    async with get_client() as client:
        existing_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                name=FlowRunFilterName(like_=f"scrape-{network}-{proposal_id}"),
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(
                        any_=[
                            StateType.RUNNING,
                            StateType.COMPLETED, # If automatically triggered, we don't re-scape. Gotta be done manually. 
                            StateType.PENDING,
                            StateType.SCHEDULED,
                        ]
                    )
                ),
            ),
            deployment_filter=DeploymentFilter(
                id=DeploymentFilterId(any_=[DATA_SCRAPER_DEPLOYMENT_ID])
            ),
        )

    if existing_runs:
        logger.warning(
            f"Found {len(existing_runs)} existing run(s) for proposal {proposal_id} on '{network}'. Skipping scheduling."
        )
        return True

    logger.info(
        f"No existing runs found for proposal {proposal_id} on '{network}'. It's safe to schedule."
    )
    return False


@task
async def schedule_scraping_task(proposal_id: int, network: str):
    """Schedules the cybergov_scraper flow to run in the future."""
    logger = get_run_logger()

    delay = datetime.timedelta(days=SCRAPING_SCHEDULE_DELAY_DAYS)
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + delay
    logger.info(
        f"Scheduling scraper for proposal {proposal_id} on '{network}' "
        f"to run at {scheduled_time.isoformat()}"
    )

    async with get_client() as client:
        await client.create_flow_run_from_deployment(
            name=f"scrape-{network}-{proposal_id}",
            deployment_id=DATA_SCRAPER_DEPLOYMENT_ID,
            parameters={"proposal_id": proposal_id, "network": network},
            state=Scheduled(),
            # state=Scheduled(scheduled_time=scheduled_time) #
        )


# --- The Main Dispatcher Flow ---
@flow(name="Cybergov Proposal Dispatcher", log_prints=True)
async def cybergov_dispatcher_flow(
    # networks: List[str] = ["polkadot", "kusama", "paseo"],
    networks: List[str] = ["paseo"],
    proposal_id: Optional[int] = None,
    network: Optional[str] = None,
):
    """
    Checks for new proposals using Scaleway S3, then schedules scraping tasks.
    """
    logger = get_run_logger()

    s3_bucket_block = await String.load("scaleway-bucket-name")
    endpoint_block = await String.load("scaleway-s3-endpoint-url")
    access_key_block = await Secret.load("scaleway-access-key-id")
    secret_key_block = await Secret.load("scaleway-secret-access-key")

    s3_bucket = s3_bucket_block.value
    endpoint_url = endpoint_block.value
    access_key = access_key_block.get()
    secret_key = secret_key_block.get()

    if proposal_id is not None and network is not None:
        logger.warning(
            f"MANUAL OVERRIDE: Scheduling single proposal {proposal_id} on '{network}'."
        )
        await schedule_scraping_task(proposal_id=proposal_id, network=network)
        return

    logger.info(f"Running in scheduled mode for networks: {networks}")
    for net in networks:
        last_known_id = await get_last_processed_id_from_s3(
            network=net,
            s3_bucket=s3_bucket,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
        )
        new_proposals = await find_new_proposals(
            network=net, last_known_id=last_known_id
        )
        if not new_proposals:
            logger.info(f"No new proposals to schedule for '{net}'.")
            continue

        for proposal in new_proposals:
            p_id = proposal["proposalIndex"]
            is_already_scheduled = await check_if_already_scheduled(
                proposal_id=p_id, network=net
            )
            if not is_already_scheduled:
                await schedule_scraping_task(proposal_id=p_id, network=net)


if __name__ == "__main__":
    import asyncio
    asyncio.run(
        cybergov_dispatcher_flow(
            network="paseo", 
            proposal_id=100
        )
    )
