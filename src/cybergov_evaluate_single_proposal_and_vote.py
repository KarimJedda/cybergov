import os
import sys
import json
import datetime
from typing import Dict

# You will need s3fs installed in your GitHub Action's environment
# (add it to your requirements.txt)
import s3fs

def get_config_from_env() -> Dict[str, str]:
    """
    Retrieves all necessary configuration from environment variables.
    Fails the script if any required variable is missing.
    """
    required_vars = [
        "PROPOSAL_ID",
        "NETWORK",
        "S3_BUCKET",
        # Add AWS credentials if not using instance roles in a self-hosted runner
        # "AWS_ACCESS_KEY_ID",
        # "AWS_SECRET_ACCESS_KEY",
    ]
    config = {}
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        config[var.lower()] = value

    if missing_vars:
        print(f"FATAL: Missing required environment variables: {', '.join(missing_vars)}", file=sys.stderr)
        sys.exit(1)
        
    print("✓ Configuration loaded successfully from environment variables.")
    return config

def update_metadata_status(s3_fs: s3fs.S3FileSystem, s3_path: str, status: str, extra_data: Dict = None):
    """
    Reads, updates, and writes the metadata.json file in S3.
    This function is the primary state machine for the proposal.
    """
    metadata_path = f"{s3_path}/metadata.json"
    print(f"Updating status to '{status}' in {metadata_path}...")

    metadata = {}
    # Try to load existing metadata if it exists
    if s3_fs.exists(metadata_path):
        with s3_fs.open(metadata_path, 'r') as f:
            metadata = json.load(f)

    # Update fields
    metadata['status'] = status
    metadata['last_updated_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if extra_data:
        metadata.update(extra_data)

    # Write back to S3
    with s3_fs.open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✓ Successfully updated status to '{status}'.")


def evaluate_proposal(config: Dict, s3_fs: s3fs.S3FileSystem, proposal_s3_path: str):
    """
    DUMMY SKELETON: Performs the data scraping and evaluation steps.
    In the real implementation, this would involve:
    1. Calling the Subsquare API to get proposal details.
    2. Cleaning the content.
    3. Running LLM analyses.
    4. Generating a consensus report.
    """
    print("\n--- Starting Proposal Evaluation ---")
    
    # --- 1. Scrape Raw Data (e.g., from Subsquare) ---
    print("DUMMY: Scraping raw data from Subsquare...")
    raw_subsquare_data = {
        "proposal_id": config["proposal_id"],
        "network": config["network"],
        "title": f"A Very Important Proposal #{config['proposal_id']}",
        "content_raw": "This is the full text of the proposal...",
        "proposer": "1ABC...XYZ",
        "requested_amount": 1000,
        "fetched_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    with s3_fs.open(f"{proposal_s3_path}/raw_subsquare.json", 'w') as f:
        json.dump(raw_subsquare_data, f, indent=2)
    print("✓ Saved raw_subsquare.json to S3.")

    # --- 2. Create Cleaned Content ---
    print("DUMMY: Cleaning and extracting content for LLMs...")
    cleaned_content = f"# Proposal {config['proposal_id']}\n\n{raw_subsquare_data['content_raw']}"
    with s3_fs.open(f"{proposal_s3_path}/content.md", 'w') as f:
        f.write(cleaned_content)
    print("✓ Saved content.md to S3.")

    # --- 3. Run LLM Analyses (dummy outputs) ---
    print("DUMMY: Generating dummy LLM analysis files...")
    llm_path = f"{proposal_s3_path}/llm_analyses"
    s3_fs.makedirs(llm_path, exist_ok=True)
    for llm_name in ["balthazar", "caspar", "melchior"]:
        analysis = { "recommendation": "APPROVE", "confidence": 0.85, "reasoning": "Looks good." }
        with s3_fs.open(f"{llm_path}/{llm_name}.json", 'w') as f:
            json.dump(analysis, f, indent=2)
    print("✓ Saved dummy LLM analyses to S3.")
    
    print("--- ✓ Proposal Evaluation Complete ---")


def submit_vote(config: Dict, s3_fs: s3fs.S3FileSystem, proposal_s3_path: str):
    """
    DUMMY SKELETON: Submits the on-chain vote.
    In the real implementation, this would involve:
    1. Loading a wallet/private key from GitHub secrets.
    2. Constructing the transaction payload.
    3. Signing and submitting the transaction to a node.
    4. Waiting for the transaction to be finalized.
    """
    print("\n--- Starting On-Chain Vote ---")
    
    # This is the core logic requested in the prompt
    print(f"VOTING on proposal {config['proposal_id']} on network {config['network']}")
    
    # --- Save a dummy vote receipt ---
    # The real version would contain the actual transaction hash
    vote_receipt = {
        "network": config["network"],
        "proposal_id": config["proposal_id"],
        "vote": "Aye",
        "transaction_hash": f"0x{os.urandom(32).hex()}",
        "block_hash": f"0x{os.urandom(32).hex()}",
        "voted_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    with s3_fs.open(f"{proposal_s3_path}/vote_receipt.json", 'w') as f:
        json.dump(vote_receipt, f, indent=2)
    print(f"✓ Saved vote_receipt.json to S3 with tx_hash: {vote_receipt['transaction_hash']}")
    
    print("--- ✓ On-Chain Vote Complete ---")


def main():
    """Main execution function."""
    print("======================================================")
    print("  CyberGov Proposal Evaluator and Voter Initializing  ")
    print("======================================================")
    
    config = get_config_from_env()
    proposal_id = config["proposal_id"]
    network = config["network"]
    bucket = config["s3_bucket"]

    # Initialize the S3 filesystem client
    s3_fs = s3fs.S3FileSystem()
    
    # Define the base S3 path for all artifacts related to this proposal
    proposal_s3_path = f"{bucket}/proposals/{network}/{proposal_id}"
    
    # Ensure the base directory exists
    s3_fs.makedirs(proposal_s3_path, exist_ok=True)
    
    # The main try/except block ensures that we always update the final status
    try:
        # 1. Set status to 'processing' as the very first step
        update_metadata_status(s3_fs, proposal_s3_path, "processing")
        
        # 2. Perform the evaluation (data scraping, LLM analysis, etc.)
        evaluate_proposal(config, s3_fs, proposal_s3_path)
        
        # 3. Perform the vote
        submit_vote(config, s3_fs, proposal_s3_path)

        # 4. If all steps succeed, set the final status to 'completed'
        final_data = {"vote_transaction": s3_fs.glob(f"{proposal_s3_path}/vote_receipt.json")[0]}
        update_metadata_status(s3_fs, proposal_s3_path, "completed", extra_data=final_data)
        
        print("\n🎉 Proposal processing and voting finished successfully!")

    except Exception as e:
        print(f"\n💥 FATAL ERROR during processing: {e}", file=sys.stderr)
        # On any failure, update the metadata with an error message
        error_data = {"error_message": str(e)}
        update_metadata_status(s3_fs, proposal_s3_path, "failed", extra_data=error_data)
        sys.exit(1)


if __name__ == "__main__":
    main()