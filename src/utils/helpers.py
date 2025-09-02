import logging
import os
import sys
from typing import Dict
import hashlib


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


def get_config_from_env() -> Dict[str, str]:
    logger = setup_logging()

    required_vars = [
        "PROPOSAL_ID",
        "NETWORK",
        "S3_BUCKET_NAME",
        "S3_ACCESS_KEY_ID",
        "S3_ACCESS_KEY_SECRET",
        "S3_ENDPOINT_URL",
    ]
    config = {}
    missing_vars = []

    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        config[var.upper()] = value

    if missing_vars:
        logger.error(
            f"ABORTING: Missing required environment variables: {', '.join(missing_vars)}"
        )
        sys.exit(1)

    logger.info("✓ Configuration loaded successfully from environment variables.")
    return config


def hash_file(filepath, algorithm="sha256"):
    """
    Calculates the hash of a file.
    Returns a string in the format 'algorithm:hex_digest'.
    """
    h = hashlib.new(algorithm)
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return f"{algorithm}:{h.hexdigest()}"
