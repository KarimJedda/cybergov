import json
import sys
from bs4 import BeautifulSoup
import httpx
from .helpers import setup_logging

NETWORK_MAP = {
    "polkadot": "https://polkadot.subsquare.io",
    "kusama": "https://kusama.subsquare.io",
    "paseo": "https://paseo.subsquare.io"
}

class ProposalFetchError(Exception):
    pass

def run(s3, proposal_s3_path, network, ref_id):
    """
    Fetches and parses a Subsquare referenda page to extract its JSON data using httpx.
    """
    logger = setup_logging()
    base_url = NETWORK_MAP[network]
    url = f"{base_url}/referenda/{ref_id}"
    headers = {
        "User-Agent": "CybergovScraper/v0"
    }
    output_filename = f"{proposal_s3_path}/raw_subsquare_data.json"

    try:
        with httpx.Client() as client:
            logger.info(f"\tFetching data from: {url}")
            response = client.get(url, headers=headers)
            response.raise_for_status()  # Checks for 4xx or 5xx status codes

        logger.info("\tParsing HTML to find the '__NEXT_DATA__' script tag...")
        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

        if not script_tag:
            raise ValueError("\tCould not find the script tag with id='__NEXT_DATA__'. The page structure might have changed.")

        logger.info(f"\tExtracting JSON and saving to {output_filename}...")
        json_data_text = script_tag.string
        raw_subsquare_data = json.loads(json_data_text)
        
        with s3.open(output_filename, 'w') as f:
            json.dump(raw_subsquare_data, f, indent=2)

        logger.info(f"\t✅ Success! Proposal data saved to {output_filename}")

    except httpx.RequestError as e:
        logger.error(f"\t❌ Error: Failed to fetch the URL {url}.")
        raise ProposalFetchError()
    except (ValueError, AttributeError, json.JSONDecodeError) as e:
        logger.error(f"\t❌ Error: Failed to parse the page or JSON data.")
        raise ProposalFetchError()
    except Exception as e:
        logger.error(f"\t❌ An unexpected error occurred.  Reason: {e}")
        raise ProposalFetchError()