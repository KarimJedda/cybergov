import json
from typing import Dict, Any, Optional

from bs4 import BeautifulSoup
import httpx
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret, String
from prefect.tasks import exponential_backoff
import s3fs

NETWORK_MAP = {
    "polkadot": "https://polkadot.subsquare.io",
    "kusama": "https://kusama.subsquare.io",
    "paseo": "https://paseo.subsquare.io"
}

class ProposalFetchError(Exception):
    pass

class ProposalParseError(Exception):
    pass

@task(
    name="Fetch Proposal HTML from Subsquare",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.2
)
def fetch_subsquare_proposal_data(url: str) -> str:
    """Fetches the raw HTML content from a given URL."""
    logger = get_run_logger()
    user_agent_secret = Secret.load_sync("cybergov-scraper-user-agent")

    headers = {"User-Agent": user_agent_secret.get()}
    
    logger.info(f"Fetching data from: {url}")
    try:
        with httpx.Client() as client:
            response = client.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            logger.info(f"Successfully fetched HTML from {url}")
            return response.text
    except httpx.RequestError as e:
        logger.error(f"HTTP Request failed for URL {url}: {e}")
        raise ProposalFetchError(f"Failed to fetch {url}") from e


@task(name="Extract JSON from HTML")
def extract_json_from_html(html_content: str) -> Dict[str, Any]:
    """Parses HTML to find and extract the '__NEXT_DATA__' JSON blob."""
    logger = get_run_logger()
    logger.info("Parsing HTML to find the '__NEXT_DATA__' script tag...")
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        
        if not script_tag or not script_tag.string:
            raise ValueError("Could not find a valid script tag with id='__NEXT_DATA__'.")
            
        json_data = json.loads(script_tag.string)
        logger.info("Successfully extracted JSON data.")
        return json_data
    except (ValueError, json.JSONDecodeError) as e:
        logger.error(f"Failed to parse page or JSON data: {e}")
        raise ProposalParseError("The page structure might have changed or JSON is malformed.") from e


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
    proposal_url = f"{base_url}/referenda/{proposal_id}"
    s3_output_path = f"{s3_bucket}/proposals/{network}/{proposal_id}/raw_subsquare_data.json"

    subsquare_html = fetch_subsquare_proposal_data(proposal_url)
    proposal_data = extract_json_from_html(subsquare_html)
    
    save_to_s3(
        data=proposal_data,
        s3_bucket=s3_bucket,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        full_s3_path=s3_output_path
    )

    return s3_output_path


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

    except (ProposalFetchError, ProposalParseError) as e:
        logger.error(f"Pipeline failed for {network} ref {proposal_id}. Reason: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred in the pipeline for {network} ref {proposal_id}: {e}")
        raise