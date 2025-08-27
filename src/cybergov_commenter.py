import httpx
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from substrateinterface import Keypair
import json 
import time 

@task
def fetch_proposal_infos(
    network: str,
    proposal_id: int,
):
    pass 


@task
def post_comment_to_subsquare(
    network: str,
    proposal_id: int,
    proposed_height: int, 
    comment: str
):
    """
    Posts a comment to a Subsquare referendum.
    """

    logger = get_run_logger()

    api_url = f"https://{network}-api.subsquare.io/sima/referenda/{proposal_id}/comments"

    entity_payload = {
        "action": "comment",
        "indexer": {
            "pallet": "referenda",
            "object": "referendumInfoFor",
            "proposed_height": proposed_height,
            "id": proposal_id,
        },
        "content": comment,
        "content_format": "subsquare_md",
        "timestamp": int(time.time() * 1000)
    }

    message_to_sign = json.dumps(entity_payload, separators=(',', ':'))

    cybergov_mnemonic = Secret.load(f"{network}-cybergov-mnemonic").get()
    keypair = Keypair.create_from_mnemonic(cybergov_mnemonic)
    signature = keypair.sign(message_to_sign)

    user_agent_secret = Secret.load("cybergov-scraper-user-agent")
    user_agent = user_agent_secret.get()

    final_request_body = {
        "entity": entity_payload,
        "address": keypair.ss58_address,
        "signature": '0x' + signature.hex(),
        "signerWallet": "py-polkadot-sdk",
    }
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": user_agent,
    }

    logger.info(f"Sending comment request to Subsquare: {json.dumps(final_request_body)}")

    try:
        response = httpx.post(api_url, headers=headers, json=final_request_body)
        response.raise_for_status()
        logger.info(f"Success!: {response.json()}")

    except httpx.RequestError as e:
        logger.error(f"An error occurred while connecting to Subsquare: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while posting the comment to Subsquare: {e}")
        raise


@flow(name="Post comment on Subsquare", log_prints=True)
def post_magi_comment_to_subsquare(
    network: str,
    proposal_id: int,
):
    """
    A full workflow to comment on a Polkadot OpenGov proposal.
    """
    logger = get_run_logger()

    logger.info(f"Posting comment to Subsquare on network {network} for proposal {proposal_id}")

    post_comment_to_subsquare(
        network=network,
        proposal_id=proposal_id,
        proposed_height=7902563,
        comment="Automated comment posted from a bot."
    )


if __name__ == "__main__":
    post_magi_comment_to_subsquare(
        network="paseo",
        proposal_id=100, 
    )