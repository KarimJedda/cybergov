import httpx
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from substrateinterface import Keypair, SubstrateInterface

# Mapping for user-friendly conviction input
CONVICTION_MAPPING = {
    0: "None",
    1: "Locked1x",
    2: "Locked2x",
    3: "Locked3x",
    4: "Locked4x",
    5: "Locked5x",
    6: "Locked6x",
}


@task
def create_and_sign_vote_tx(
    proposal_id: int,
    network: str,
    vote_aye: bool,
    conviction: int,
    remark_text: str,
) -> str:
    """
    Creates and signs a batch transaction to vote on an OpenGov proposal
    and include a system remark.
    """
    logger = get_run_logger()

    if conviction not in CONVICTION_MAPPING:
        raise ValueError(f"Invalid conviction value '{conviction}'. Must be one of {list(CONVICTION_MAPPING.keys())}")

    network_rpc_block = Secret.load(f"{network}-rpc-url")
    network_rpc_url = network_rpc_block.get()

    logger.info(
        f"Connecting to Sidecar node for {network} to prepare vote..."
    )

    try:
        with SubstrateInterface(url=network_rpc_url) as substrate:
            try:
                mnemonic = Secret.load(f"{network}-cybergov-mnemonic").get()
                keypair = Keypair.create_from_mnemonic(mnemonic)
                logger.info(f"Loaded keypair for address: {keypair.ss58_address}")
            except ValueError:
                logger.error(f"Could not load '{network}-voter-mnemonic' Secret block.")
                raise

            vote = {
                "Standard": {
                    "vote": {"aye": vote_aye, "conviction": CONVICTION_MAPPING[conviction]},
                    ## TODO: how much to vote with? 
                    "balance": 3500 * 10**10
                }
            }
            vote_call = substrate.compose_call(
                call_module="ConvictionVoting",
                call_function="vote",
                call_params={"poll_index": proposal_id, "vote": vote},
            )

            remark_call = substrate.compose_call(
                call_module="System",
                call_function="remark_with_event",
                call_params={"remark": remark_text.encode()},
            )
            
            logger.info("Composing utility.batch call with vote and remark.")
            batch_call = substrate.compose_call(
                call_module="Utility",
                call_function="batch_all",
                call_params={"calls": [vote_call, remark_call]}
            )

            extrinsic = substrate.create_signed_extrinsic(call=batch_call, keypair=keypair)
            signed_tx_hex = str(extrinsic.data)
            logger.info(f"Successfully created and signed transaction.")
            
            return signed_tx_hex

    except Exception as e:
        logger.error(f"An error occurred during transaction creation: {e}")
        raise


@task
def submit_transaction_sidecar(network: str, tx_hex: str) -> str:
    """
    Submits the signed transaction using the Substrate Sidecar.

    Returns:
        The transaction hash.
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
        logger.error(f"Submission failed with status {response.status_code}: {response.text}")
        response.raise_for_status()

    tx_hash = response.json().get("hash", None)
    if not tx_hash:
        raise ValueError(f"Could not find 'hash' in submission response: {response.json()}")
        
    logger.info(f"Transaction submitted successfully! Hash: {tx_hash}")
    return tx_hash


@flow(name="Vote on Polkadot OpenGov", log_prints=True)
def vote_on_opengov_proposal(
    network: str,
    proposal_id: int,
    vote_aye: bool = True,
    conviction: int = 1,
    remark_text: str = "Voted via Prefect",
):
    """
    A full workflow to vote on a Polkadot OpenGov proposal.
    """
    logger = get_run_logger()

    signed_tx = create_and_sign_vote_tx(
        network=network,
        proposal_id=proposal_id,
        vote_aye=vote_aye,
        conviction=conviction,
        remark_text=remark_text,
    )


    tx_hash = submit_transaction_sidecar(
        network=network,
        tx_hex=signed_tx, 
    )

    logger.info(f"✅ Successfully processed vote for proposal {proposal_id}. View transaction at: https://{network}.subscan.io/extrinsic/{tx_hash}")


if __name__ == "__main__":
    # Example of how to run the flow
    vote_on_opengov_proposal(
        network="paseo",
        proposal_id=100, 
        vote_aye=True,
        conviction=6, 
        remark_text="max bidding"
    )