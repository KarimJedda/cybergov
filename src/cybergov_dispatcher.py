import asyncio
import datetime
import random
import re
from typing import List, Optional, Dict
from prefect import flow, task, get_run_logger
import s3fs

# --- Prefect Blocks Integration (Generic) ---
from prefect.blocks.system import String, Secret

# --- Configuration Constants (Scraper-specific) ---
SCRAPER_DEPLOYMENT_ID = "595089d8-5f04-46e5-91b7-c8f6935275fc" # TODO make this a var or something
SCHEDULE_DELAY_DAYS = 2


# --- S3 and API Tasks ---
@task
async def get_last_processed_id_from_s3(
    network: str,
    s3_bucket: str,
    access_key: str,
    secret_key: str,
    endpoint_url: str
) -> int:
    """
    Checks S3-compatible storage (like Scaleway) using credentials and a specific endpoint_url.
    """
    logger = get_run_logger()
    s3_path = f"{s3_bucket}/proposals/{network}/"
    logger.info(f"Checking for existing proposals in S3-compatible storage path: s3://{s3_path}")
    logger.info(f"Using S3 endpoint: {endpoint_url}")

    try:
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint_url,
            }
        )

        existing_proposal_paths = s3.find(s3_path, maxdepth=1)

        proposal_ids = []
        for path in existing_proposal_paths:
            match = re.search(r'/(\d+)$', path)
            if match:
                proposal_ids.append(int(match.group(1)))

        if not proposal_ids:
            logger.info(f"No existing proposals found for '{network}'. Starting from 0.")
            return 0

        max_id = max(proposal_ids)
        logger.info(f"Highest proposal ID found for '{network}' is {max_id}.")
        return max_id

    except FileNotFoundError:
        logger.info(f"S3 path s3://{s3_path} does not exist. Assuming first run for '{network}'.")
        return 0
    except Exception as e:
        logger.error(f"Failed to list S3 directory 's3://{s3_path}': {e}")
        raise

# ... (The find_new_proposals and schedule_scraping_task tasks remain unchanged) ...
@task
async def find_new_proposals(network: str, last_id: int) -> List[Dict]:
    """DUMMY: Simulates fetching proposals from Subsquare with an index > last_id."""
    logger = get_run_logger()
    logger.info(f"DUMMY: Checking for new proposals on '{network}' after ID {last_id}...")
    num_new = 1
    if num_new == 0:
        return []
    new_proposals = [{"proposalIndex": last_id + i + 1} for i in range(num_new)]
    logger.info(f"DUMMY: Found {len(new_proposals)} new proposals for '{network}'.")
    return new_proposals

@task
async def schedule_scraping_task(proposal_id: int, network: str):
    """Schedules the cybergov_scraper flow to run in the future."""
    logger = get_run_logger()
    from prefect.client.orchestration import get_client
    from prefect.states import Scheduled

    delay = datetime.timedelta(days=SCHEDULE_DELAY_DAYS)
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + delay
    logger.info(
        f"Scheduling scraper for proposal {proposal_id} on '{network}' "
        f"to run at {scheduled_time.isoformat()}"
    )

    async with get_client() as client:
        await client.create_flow_run_from_deployment(
            name=f"scrape-{network}-{proposal_id}",
            deployment_id=SCRAPER_DEPLOYMENT_ID,
            parameters={"proposal_id": proposal_id, "network": network},
            state=Scheduled()
            #state=Scheduled(scheduled_time=scheduled_time)
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
            endpoint_url=endpoint_url
        )
        new_proposals = await find_new_proposals(network=net, last_id=last_known_id)
        if not new_proposals:
            logger.info(f"No new proposals to schedule for '{net}'.")
            continue

        for proposal in new_proposals:
            p_id = proposal["proposalIndex"]
            await schedule_scraping_task(proposal_id=p_id, network=net)
