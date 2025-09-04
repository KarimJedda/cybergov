import httpx
from prefect import flow, get_run_logger, task
from substrateinterface import Keypair
import json
import time
from prefect.blocks.system import String, Secret
import s3fs


@task
def get_infos_for_substrate_comment(
    network: str,
    proposal_id: int,
    s3_bucket: str,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
):
    """
    Fetch MAGI vote result from S3 and execute vote
    Returns:
        vote_result: aye, nay or abstain
        comment: The summary / rationale that will be posted
        proposal_height: the block at which the proposal was submitted
    """
    logger = get_run_logger()
    logger.info(f"Checking for vote results on {network} for proposal {proposal_id}")

    # Define file paths
    base_path = f"{s3_bucket}/proposals/{network}/{proposal_id}"
    vote_file_path = f"{base_path}/vote.json"
    subsquare_file_path = f"{base_path}/raw_subsquare_data.json"

    try:
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint_url,
            },
        )

        # Get proposal_height from raw_subsquare_data.json
        logger.info(f"Loading subsquare data from {subsquare_file_path}")
        with s3.open(subsquare_file_path, "rb") as f:
            subsquare_data = json.load(f)

        proposal_height = subsquare_data.get("indexer", {}).get("blockHeight")
        if proposal_height is None:
            logger.warning(
                f"Could not find 'indexer.blockHeight' in {subsquare_file_path}"
            )
        else:
            logger.info(f"Found proposal height: {proposal_height}")

        # Get vote_result and comment from vote.json
        logger.info(f"Loading vote data from {vote_file_path}")
        with s3.open(vote_file_path, "rb") as f:
            vote_data = json.load(f)
        logger.info(f"Successfully loaded vote data from {vote_file_path}")

        # Assuming the vote decision is stored under the key 'vote_decision'
        # vote_result = vote_data.get("vote_decision")
        comment = vote_data.get("summary_rationale", "")

        # logger.info(f"Vote result for {proposal_id} on {network}")
        # logger.info(f"Comment for {proposal_id} on {network}: {comment}.")

        return comment, proposal_height

    except FileNotFoundError as e:
        logger.info(
            f"A required file was not found ({e}). No inference result available yet."
        )
        return None, None, None
    except Exception as e:
        logger.error(
            f"Failed to process files for proposal {proposal_id} on {network} due to an unexpected error: {e}"
        )
        raise


@task
def post_comment_to_subsquare(
    network: str, proposal_id: int, proposed_height: int, comment: str
):
    """
    Posts a comment to a Subsquare referendum.
    """

    logger = get_run_logger()

    api_url = (
        f"https://{network}-api.subsquare.io/sima/referenda/{proposal_id}/comments"
    )

    entity_payload = {
        "action": "comment",
        "indexer": {
            "pallet": "referenda",
            "object": "referendumInfoFor",
            "proposed_height": proposed_height,
            "id": proposal_id,
        },
        "content": comment,
        "content_format": "HTML",
        "timestamp": int(time.time() * 1000),
    }

    message_to_sign = json.dumps(
        entity_payload, 
        sort_keys=True, 
        separators=(",", ":"),
        ensure_ascii=False
    )

    cybergov_mnemonic = Secret.load(f"{network}-cybergov-mnemonic").get()
    keypair = Keypair.create_from_mnemonic(cybergov_mnemonic)
    signature = keypair.sign(message_to_sign)

    user_agent_secret = Secret.load("cybergov-scraper-user-agent")
    user_agent = user_agent_secret.get()

    final_request_body = {
        "entity": entity_payload,
        "address": keypair.ss58_address,
        "signature": "0x" + signature.hex(),
        "signerWallet": "py-polkadot-sdk",
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": user_agent,
    }

    logger.info(
        f"Sending comment request to Subsquare: {json.dumps(final_request_body)}"
    )

    try:
        # This is absolutely crucial here!
        # posting comment_manual='TEST Again using a short message and some quotes " \n test " " ' DOES NOT WORK
        # posting comment_manual='TEST Again using a short message' WORKS
        # previously we were sending json=final_request_body, but changing it to purely data and let the server figure things out works better
        response = httpx.post(api_url, headers=headers, data=json.dumps(final_request_body, sort_keys=True, separators=(",", ":")))
        response.raise_for_status()
        logger.info(f"Success!: {response.json()}")

    except httpx.RequestError as e:
        logger.error(f"An error occurred while connecting to Subsquare: {e}")
        raise
    except httpx.HTTPStatusError as e:
        logger.error(f"An error occured with the POST request.")
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

    s3_bucket_block = String.load("scaleway-bucket-name")
    endpoint_block = String.load("scaleway-s3-endpoint-url")
    access_key_block = Secret.load("scaleway-access-key-id")
    secret_key_block = Secret.load("scaleway-secret-access-key")

    s3_bucket = s3_bucket_block.value
    endpoint_url = endpoint_block.value
    access_key = access_key_block.get()
    secret_key = secret_key_block.get()

    logger.info(
        f"Posting comment to Subsquare on network {network} for proposal {proposal_id}"
    )

    comment, proposal_height = get_infos_for_substrate_comment(
        network=network,
        proposal_id=proposal_id,
        s3_bucket=s3_bucket,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )

    if comment:
        post_comment_to_subsquare(
            network=network,
            proposal_id=proposal_id,
            proposed_height=proposal_height,
            comment=comment,
        )
        logger.info(f"✅ Successfully posted comment for {proposal_id} on {network}")
    else:
        logger.error("Cannot post comment, no content provided.")
        raise


if __name__ == "__main__":
    import sys
    import asyncio

    if len(sys.argv) != 3:
        print("Usage: python cybergov_commenter.py <network> <proposal_id>")
        sys.exit(1)

    network_arg = sys.argv[1]
    proposal_id_arg = int(sys.argv[2])

    post_magi_comment_to_subsquare(
            network=network_arg, 
            proposal_id=proposal_id_arg,
        )