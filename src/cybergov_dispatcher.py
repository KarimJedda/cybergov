import asyncio
import datetime
import random
import re
from typing import List, Optional, Dict

from prefect import flow, task, get_run_logger
import s3fs

# --- Configuration Constants ---
S3_BUCKET = "your-bucket-name"  # <-- IMPORTANT: Change this
SCRAPER_DEPLOYMENT_NAME = "Proposal Scraper/proposal-scraper-deployment"
SCHEDULE_DELAY_DAYS = 2

# --- S3 and API Tasks ---

@task
async def get_last_processed_id_from_s3(network: str) -> int:
    """
    Checks S3 to find the highest proposal ID already processed for a given network.
    This replaces the Prefect Block for state management.
    """
    logger = get_run_logger()
    s3_path = f"{S3_BUCKET}/proposals/{network}/"
    logger.info(f"Checking for existing proposals in S3 path: s3://{s3_path}")
    
    try:
        # s3fs will use credentials from the environment (e.g., AWS_ACCESS_KEY_ID)
        s3 = s3fs.S3FileSystem()
        # `find` is efficient for listing directories
        existing_proposal_paths = s3.find(s3_path, maxdepth=1)
        
        # Extract proposal IDs (numbers) from the directory paths
        proposal_ids = []
        for path in existing_proposal_paths:
            # Match paths like 'your-bucket-name/proposals/polkadot/123'
            match = re.search(r'/(\d+)$', path)
            if match:
                proposal_ids.append(int(match.group(1)))

        if not proposal_ids:
            logger.info(f"No existing proposals found in S3 for '{network}'. Starting from 0.")
            return 0
            
        max_id = max(proposal_ids)
        logger.info(f"Highest proposal ID found in S3 for '{network}' is {max_id}.")
        return max_id
        
    except FileNotFoundError:
        logger.info(f"S3 path s3://{s3_path} does not exist. Assuming first run for '{network}'.")
        return 0
    except Exception as e:
        logger.error(f"Failed to list S3 directory 's3://{s3_path}': {e}")
        # Fail the task to prevent scheduling incorrect jobs
        raise

@task
async def find_new_proposals(network: str, last_id: int) -> List[Dict]:
    """DUMMY: Simulates fetching proposals from Subsquare with an index > last_id."""
    logger = get_run_logger()
    logger.info(f"DUMMY: Checking for new proposals on '{network}' after ID {last_id}...")
    num_new = random.randint(0, 3)
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
    from prefect.server.schemas.states import Scheduled

    delay = datetime.timedelta(days=SCHEDULE_DELAY_DAYS)
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + delay
    
    logger.info(
        f"Scheduling scraper for proposal {proposal_id} on '{network}' "
        f"to run at {scheduled_time.isoformat()}"
    )
    
    async with get_client() as client:
        await client.create_flow_run_from_deployment(
            deployment_name=SCRAPER_DEPLOYMENT_NAME,
            parameters={"proposal_id": proposal_id, "network": network},
            state=Scheduled(scheduled_time=scheduled_time),
        )

# --- The Main Dispatcher Flow ---

@flow(name="Cybergov Proposal Dispatcher", log_prints=True)
async def cybergov_dispatcher_flow(
    networks: List[str] = ["polkadot", "kusama", "testnet"],
    proposal_id: Optional[int] = None,
    network: Optional[str] = None,
):
    """
    Checks for new proposals by comparing S3 state with an external API, then
    schedules scraping tasks for any new proposals found.
    
    Supports manual override for reprocessing a single proposal.
    """
    logger = get_run_logger()

    # MANUAL OVERRIDE MODE for backfilling/reprocessing
    if proposal_id is not None and network is not None:
        logger.warning(
            f"MANUAL OVERRIDE: Scheduling single proposal {proposal_id} on '{network}'."
        )
        await schedule_scraping_task(proposal_id=proposal_id, network=network)
        return

    # SCHEDULED MODE
    logger.info(f"Running in scheduled mode for networks: {networks}")
    for net in networks:
        last_known_id = await get_last_processed_id_from_s3(network=net)
        new_proposals = await find_new_proposals(network=net, last_id=last_known_id)

        if not new_proposals:
            logger.info(f"No new proposals to schedule for '{net}'.")
            continue

        for proposal in new_proposals:
            p_id = proposal["proposalIndex"]
            await schedule_scraping_task(proposal_id=p_id, network=net)

if __name__ == "__main__":
    cybergov_dispatcher_flow.serve(
        name="cybergov-dispatcher-deployment",
        cron="*/30 * * * *"  # Runs every 30 minutes
    )