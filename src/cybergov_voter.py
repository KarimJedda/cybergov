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


class DryRunException(Exception):
    pass

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

# There is a bug with sidecar dry run
#20:35:37.576 | ERROR   | Task run 'dry_run_transaction_sidecar-158' - Dry-run failed with status 400: {"code":400,"error":"Unable to dry-run transaction","transaction":"0x3902840010bdba8f7410bca707bb272330a00030348bcc7abb87d521c2c7b8e00af51d5c01662cdfd8c2ea68078b1ea219d109460daaebaa75d79ec510a53314fcfba2d33345fc217272cd58e9c6cfe6c782abf54a508bc7628fdf56779ee7561f94179384000400000503008eaf04151687736326c9fea17e25fc5287613693c912909cb226aa4794f26a4802286bee","cause":"createType(PaseoRuntimeRuntimeCall):: findMetaCall: Unable to find Call with index [57, 2]/[57,2]","stack":"Error: createType(PaseoRuntimeRuntimeCall):: findMetaCall: Unable to find Call with index [57, 2]/[57,2]\n    at createTypeUnsafe (/usr/src/app/node_modules/@polkadot/types-create/cjs/create/type.js:54:22)\n    at TypeRegistry.createTypeUnsafe (/usr/src/app/node_modules/@polkadot/types/cjs/create/registry.js:230:52)\n    at /usr/src/app/node_modules/@polkadot/api/cjs/base/Decorate.js:494:110\n    at Array.map (<anonymous>)\n    at /usr/src/app/node_modules/@polkadot/api/cjs/base/Decorate.js:494:87\n    at /usr/src/app/node_modules/@polkadot/api/cjs/promise/decorateMethod.js:43:30\n    at new Promise (<anonymous>)\n    at decorateCall (/usr/src/app/node_modules/@polkadot/api/cjs/promise/decorateMethod.js:39:12)\n    at Object.dryRunCall (/usr/src/app/node_modules/@polkadot/api/cjs/promise/decorateMethod.js:73:15)\n    at TransactionDryRunService.dryRuntExtrinsic (/usr/src/app/build/src/services/transaction/TransactionDryRunService.js:55:42)","at":{"hash":"0x849632d884b91d4ba8d4a5cf92613d57b97d656c64df2ca6d9665901d36e40d5"}}
# @task
# def dry_run_transaction_sidecar(network:str, tx_hex: str):
#     """
#     Performs a dry-run of the transaction using the Substrate Sidecar.
#     Fails the task if the dry-run is not successful.
#     """
#     logger = get_run_logger()

#     network_sidecar_block = Secret.load(f"{network}-sidecar-url")
#     network_sidecar_url = network_sidecar_block.get()

#     mnemonic = Secret.load(f"{network}-cybergov-mnemonic").get()
#     keypair = Keypair.create_from_mnemonic(mnemonic)

#     url = f"{network_sidecar_url}/transaction/dry-run"
#     payload = {
#         "tx": tx_hex,
#         "senderAddress": keypair.ss58_address
#     }
    
#     logger.info(f"Performing dry-run via Sidecar at {url}...")
    
#     with httpx.Client(timeout=30) as client:
#         response = client.post(url, json=payload)

#     if response.status_code != 200:
#         logger.error(f"Dry-run failed with status {response.status_code}: {response.text}")
#         response.raise_for_status()
    
#     result = response.json()

#     payload = result.get('result', result)

#     # Now, check the unwrapped payload for known success/error structures.
#     if payload.get("resultType") == "DispatchOutcome":
#         logger.info(f"Dry-run successful: {payload}")
#         return

#     elif payload.get("resultType") == "DispatchError":
#         error_message = f"Dry-run failed with DispatchError: {payload}"
#         logger.error(error_message)
#         raise DryRunException(error_message)

#     elif "error" in payload:
#         error_cause = payload.get('cause', 'No cause provided.')
#         error_message = f"Dry-run failed with API error: {error_cause}"
#         logger.error(error_message)
#         raise DryRunException(error_message)

#     else:
#         error_message = f"Unexpected dry-run response format: {result}"
#         logger.error(error_message)
#         raise ValueError(error_message)


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

    # dry_run_transaction_sidecar(
    #     network=network,
    #     tx_hex=signed_tx
    # )

    tx_hash = submit_transaction_sidecar(
        network=network,
        tx_hex=signed_tx, 
        # wait_for=[dry_run_transaction_sidecar]
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