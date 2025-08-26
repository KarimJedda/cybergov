import sys
import json
import datetime
import s3fs

from utils.helpers import setup_logging, get_config_from_env
import utils.run_magi_eval as run_magi_eval

def main():
    logger = setup_logging()
    
    logger.info("CyberGov V0 ... initializing.")
    last_good_step = "initializing"
    
    config = get_config_from_env()
    proposal_id = config["PROPOSAL_ID"]
    network = config["NETWORK"]

    s3_bucket = config["S3_BUCKET_NAME"]
    s3_access_key = config["S3_ACCESS_KEY_ID"]
    s3_secret_key = config["S3_ACCESS_KEY_SECRET"]
    s3_endpoint_url = config["S3_ENDPOINT_URL"]

    s3 = s3fs.S3FileSystem(
        key=s3_access_key,
        secret=s3_secret_key,
        client_kwargs={
            "endpoint_url": s3_endpoint_url,
        }
    )
    
    proposal_s3_path = f"{s3_bucket}/proposals/{network}/{proposal_id}"
    logger.info(f"00 - Checking S3 at {proposal_s3_path}...")
    s3.makedirs(proposal_s3_path, exist_ok=True)
    last_good_step = "s3 buckets all-set"
    

    try:
        logger.info("01 - pre-flight data checks...")

        logger.info("02 - Running MAGI V0 Evaluation...")
        run_magi_eval.run(s3, proposal_s3_path, network, proposal_id)
        last_good_step = "running MAGI V0 Evaluation"

    except Exception as e:
        # Don't be too generous with logging, the GA will be public and could leak secrets
        logger.error(f"\n💥 FATAL ERROR during processing... Last good step: {last_good_step}")
        sys.exit(1)

if __name__ == "__main__":
    main()