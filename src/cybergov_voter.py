import httpx
from prefect import flow, get_run_logger, task
from substrateinterface import Keypair, SubstrateInterface
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
from prefect.blocks.system import String, Secret
import s3fs
import hashlib
import datetime
from enum import Enum
import json
from utils.constants import (
    COMMENTING_DEPLOYMENT_ID,
    COMMENTING_SCHEDULE_DELAY_MINUTES,
    CONVICTION_MAPPING,
    proxy_mapping,
    voting_power
)

CONVICTION_UNANIMOUS = 6
CONVICTION_DEFAULT = 1

class VoteResult(str, Enum):
    AYE = "AYE"
    NAY = "NAY"
    ABSTAIN = "ABSTAIN"

def get_remark_hash(s3_client: s3fs.S3FileSystem, file_path: str) -> str:
    """Reads a JSON file from S3, and returns its canonical SHA256 hash."""
    with s3_client.open(file_path, "rb") as f:
        manifest_data = json.load(f)
    
    # To hash consistently, dump the dict to a string with sorted keys, then encode to bytes.
    canonical_manifest = json.dumps(manifest_data, sort_keys=True, separators=(',', ':')).encode("utf-8")
    return hashlib.sha256(canonical_manifest).hexdigest()

@task
def create_and_sign_vote_tx(
    proposal_id: int,
    network: str,
    vote: str,
    conviction: int,
    remark_text: str,
) -> str:
    """
    Creates and signs a batch transaction to vote on an OpenGov proposal
    and include a system remark.
    """
    logger = get_run_logger()

    if conviction not in CONVICTION_MAPPING:
        raise ValueError(
            f"Invalid conviction value '{conviction}'. Must be one of {list(CONVICTION_MAPPING.keys())}"
        )

    network_rpc_block = Secret.load(f"{network}-rpc-url")
    network_rpc_url = network_rpc_block.get()

    logger.info(f"Connecting to Sidecar node for {network} to prepare vote...")

    try:
        with SubstrateInterface(url=network_rpc_url) as substrate:
            try:
                mnemonic = Secret.load(f"{network}-cybergov-mnemonic").get()
                keypair = Keypair.create_from_mnemonic(mnemonic)
                logger.info(f"Loaded keypair for address: {keypair.ss58_address}  / {proxy_mapping[network]['proxy']} ")
            except ValueError:
                logger.error(f"Could not load '{network}-voter-mnemonic' Secret block.")
                raise

            # The conviction vote will be submitted as a proxy
            if vote.capitalize() == "Abstain":
                vote_parameters = {
                    "SplitAbstain": {
                      "aye": 0,
                      "nay": 0,
                      "abstain": voting_power[network]
                    }
                }
            else:
                vote_parameters = {
                    "Standard": {
                        "vote": {
                            "aye": True if vote.capitalize() == "Aye" else False,
                            "conviction": CONVICTION_MAPPING[conviction],
                        },
                        "balance": voting_power[network]
                    }
                }
            vote_call = substrate.compose_call(
                call_module="ConvictionVoting",
                call_function="vote",
                call_params={"poll_index": proposal_id, "vote": vote_parameters},
            )

            proxy_vote_call = substrate.compose_call(
                call_module="Proxy",
                call_function="proxy",
                call_params={
                    "real": proxy_mapping[network]['main'],
                    "force_proxy_type": None,
                    "call": vote_call
                }
            )

            # The system remark will be submitted regularly 
            remark_call = substrate.compose_call(
                call_module="System",
                call_function="remark_with_event",
                call_params={"remark": remark_text.encode()},
            )

            logger.info("Composing utility.batch call with vote and remark.")
            batch_call = substrate.compose_call(
                call_module="Utility",
                call_function="batch_all",
                call_params={"calls": [proxy_vote_call, remark_call]},
            )

            extrinsic = substrate.create_signed_extrinsic(
                call=batch_call, keypair=keypair
            )
            signed_tx_hex = str(extrinsic.data)
            logger.info("Successfully created and signed transaction.")

            return signed_tx_hex

    except Exception as e:
        logger.error(f"An error occurred during transaction creation: {e}")
        raise


@task
def submit_transaction_sidecar(network: str, tx_hex: str) -> str:
    """
    Submits the signed transaction using the Substrate Sidecar.
    """
    logger = get_run_logger()

    network_sidecar_block = Secret.load(f"{network}-sidecar-url")
    network_sidecar_url = network_sidecar_block.get()

    url = f"{network_sidecar_url}/transaction"
    payload = {"tx": tx_hex}

    logger.info(f"Submitting transaction via Sidecar at {url}...")
    with httpx.Client(timeout=30) as client:
        response = client.post(url, json=payload)

    if response.status_code != 200:
        logger.error(
            f"Submission failed with status {response.status_code}: {response.text}"
        )
        response.raise_for_status()

    tx_hash = response.json().get("hash", None)
    if not tx_hash:
        raise ValueError(
            f"Could not find 'hash' in submission response: {response.json()}"
        )

    logger.info(f"Transaction submitted successfully! Hash: {tx_hash}")
    return tx_hash


@task
def get_inference_result(
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
        vote_result: Aye, Nay or Abstain
        conviction: how convinced (if aye or nay)
        remark_text: the hash of vote.json the data that was used to vote
    """
    logger = get_run_logger()
    vote_file_path = f"{s3_bucket}/proposals/{network}/{proposal_id}/vote.json"
    manifest_file_path = f"{s3_bucket}/proposals/{network}/{proposal_id}/manifest.json"
    logger.info(f"Checking for vote results on {network} for proposal {proposal_id}")

    try:
        s3 = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                "endpoint_url": endpoint_url,
            },
        )

        with s3.open(vote_file_path, "rb") as f:
            vote_data = json.load(f)
        logger.info(f"Successfully loaded vote data from {vote_file_path}")

        raw_vote = vote_data.get("final_decision", "").upper()
        try:
            vote_result = VoteResult(raw_vote)
        except ValueError:
            raise ValueError(f"Invalid 'final_decision' in vote.json: {raw_vote}")

        is_unanimous = vote_data.get("is_unanimous", False)
        conviction = CONVICTION_UNANIMOUS if is_unanimous else CONVICTION_DEFAULT

        remark_text = get_remark_hash(s3, manifest_file_path)
        logger.info(f"Calculated remark (SHA256 of manifest): {remark_text}")

        logger.info(
            f"Vote for proposal {proposal_id}: {vote_result.value.capitalize()} with conviction {conviction}."
        )
        return vote_result, conviction, remark_text

    except FileNotFoundError:
        logger.warning(
            f"Vote file not found at {vote_file_path}. No inference result available."
        )
        return None, None, None
    except Exception as e:
        # Log the root cause for debugging, but don't expose it to the caller.
        logger.error(
            f"Failed to process vote for proposal {proposal_id}. "
            f"Error: ({type(e).__name__}) {e}"
        )
        # Raise a new, clean exception to signal failure without the stack trace.
        raise RuntimeError(
            f"Unexpected error processing vote for proposal {proposal_id}."
        ) from None


@task
async def check_if_commenting_already_scheduled(proposal_id: int, network: str) -> bool:
    """
    Checks the Prefect API to see if a scraper run for this proposal
    already exists (in a non-failed state).
    """
    logger = get_run_logger()
    logger.info(
        f"Checking for existing flow runs for comment-{network}-{proposal_id}..."
    )

    async with get_client() as client:
        existing_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                name=FlowRunFilterName(like_=f"comment-{network}-{proposal_id}"),
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
                id=DeploymentFilterId(any_=[COMMENTING_DEPLOYMENT_ID])
            ),
        )

    if existing_runs:
        logger.warning(
            f"Found {len(existing_runs)} existing comment run(s) for proposal {proposal_id} on '{network}'. Skipping scheduling."
        )
        return True

    logger.info(
        f"No existing comment runs found for proposal {proposal_id} on '{network}'. It's safe to schedule."
    )
    return False


@task
async def schedule_comment_task(proposal_id: int, network: str):
    """Schedules the cybergov_scraper flow to run in the future."""
    logger = get_run_logger()

    delay = datetime.timedelta(minutes=COMMENTING_SCHEDULE_DELAY_MINUTES)
    scheduled_time = datetime.datetime.now(datetime.timezone.utc) + delay
    logger.info(
        f"Scheduling MAGI comment for proposal {proposal_id} on '{network}' "
        f"to run at {scheduled_time.isoformat()}"
    )

    async with get_client() as client:
        await client.create_flow_run_from_deployment(
            name=f"comment-{network}-{proposal_id}",
            deployment_id=COMMENTING_DEPLOYMENT_ID,
            parameters={"proposal_id": proposal_id, "network": network},
            # state=Scheduled(scheduled_time=scheduled_time)
        )


@flow(name="Vote on Polkadot OpenGov", log_prints=True)
async def vote_on_opengov_proposal(
    network: str,
    proposal_id: int,
):
    """
    A full workflow to vote on a Polkadot OpenGov proposal.
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

    vote_result, conviction, vote_file_hash = get_inference_result(
        network=network,
        proposal_id=proposal_id,
        s3_bucket=s3_bucket,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )

    if all([vote_result, conviction, vote_file_hash]):
        signed_tx = create_and_sign_vote_tx(
            network=network,
            proposal_id=proposal_id,
            vote=vote_result,
            conviction=conviction,
            remark_text=vote_file_hash,
        )

        tx_hash = submit_transaction_sidecar(
            network=network,
            tx_hex=signed_tx,
        )

        logger.info(
            f"✅ Successfully processed vote for proposal {proposal_id}. View transaction at: https://{network}.subscan.io/extrinsic/{tx_hash} or https://assethub-{network}.subscan.io/extrinsic/{tx_hash}"
        )

        logger.info(
            f"Proceeding to posting comment..."
        )

        is_already_scheduled = await check_if_commenting_already_scheduled(
            proposal_id=proposal_id, network=network
        )
        if not is_already_scheduled:
            await schedule_comment_task(proposal_id=proposal_id, network=network)
    else:
        logger.error(f"Cannot vote, the {network}/{proposal_id}/vote.json is invalid")
        raise RuntimeError(
            f"Unexpected error processing vote for proposal {proposal_id}."
        ) from None

if __name__ == "__main__":
    import asyncio
    asyncio.run(
        vote_on_opengov_proposal(
            network="paseo", 
            proposal_id=100
        )
    )
