import sys
import json
import datetime
import s3fs
import os 

from utils.helpers import setup_logging, get_config_from_env, hash_file
from utils.run_magi_eval import run_inference_for_proposal, setup_compiled_agent
from pathlib import Path
from collections import Counter

logger = setup_logging()


def generate_summary_rationale(votes_breakdown) -> str:
    """
    Placeholder for the LLM call to generate a summary rationale.
    """
    logger.info("--> (Placeholder) Calling LLM to generate summary rationale...")
    # This would be a real LLM call in production.
    aye_votes = sum(1 for v in votes_breakdown if v['decision'] == 'AYE')
    nay_votes = sum(1 for v in votes_breakdown if v['decision'] == 'NAY')
    return f"This is a placeholder summary. The models voted {aye_votes} AYE and {nay_votes} NAY."


def perform_preflight_checks(s3, proposal_s3_path, local_workspace):
    """
    Checks for required input files in S3 and locally.
    Downloads S3 inputs and returns their metadata for the manifest.
    """
    logger.info("01 - Performing pre-flight data checks...")
    manifest_inputs = []

    # 1. Check for raw_subsquare.json in S3
    raw_subsquare_s3_path = f"{proposal_s3_path}/raw_subsquare_data.json"
    if not s3.exists(raw_subsquare_s3_path):
        raise FileNotFoundError(f"Required file not found in S3: {raw_subsquare_s3_path}")

    with s3.open(raw_subsquare_s3_path, 'r') as f:
        raw_data = json.load(f)
    required_attrs = ['referendumIndex', 'title', 'content', 'proposer']
    if not all(attr in raw_data for attr in required_attrs):
        raise ValueError(f"raw_subsquare.json is missing one of the required attributes: {required_attrs}")
    
    # Download, hash, and record raw_subsquare.json
    local_raw_path = local_workspace / "raw_subsquare.json"
    s3.download(raw_subsquare_s3_path, str(local_raw_path))
    manifest_inputs.append({
        "logical_name": "raw_subsquare_data",
        "s3_path": raw_subsquare_s3_path,
        "hash": hash_file(local_raw_path)
    })
    logger.info("✅ raw_subsquare.json found and validated.")

    # 2. Check for content-*.md in S3
    content_md_s3_path = f"{proposal_s3_path}/content.md"
    if not s3.exists(content_md_s3_path):
        raise FileNotFoundError(f"Required file not found in S3: {content_md_s3_path}")
    
    # Download, hash, and record content.md
    local_content_path = local_workspace / Path(content_md_s3_path).name
    s3.download(content_md_s3_path, str(local_content_path))
    manifest_inputs.append({
        "logical_name": "content_markdown",
        "s3_path": content_md_s3_path,
        "hash": hash_file(local_content_path)
    })
    logger.info(f"✅ {Path(content_md_s3_path).name} found.")

    # 3. Check for local system prompts
    prompt_dir = Path("templates/system_prompts")
    magi_models = ["balthazar", "caspar", "melchior"]
    for model in magi_models:
        prompt_file = prompt_dir / f"{model}_system_prompt.md"
        if not prompt_file.exists():
            raise FileNotFoundError(f"Required local system prompt not found: {prompt_file}")
    logger.info("✅ All local system prompts found.")
    
    logger.info("Pre-flight checks passed.")
    return manifest_inputs, local_content_path, magi_models


def run_magi_evaluations(magi_models, local_workspace):
    """
    Runs actual LLM evaluations using the dspy inference module.
    The function signature and its role in the pipeline remain unchanged.
    """
    logger.info("02 - Running MAGI V0 Evaluation with live models...")
    analysis_dir = local_workspace / "llm_analyses"
    analysis_dir.mkdir(exist_ok=True)

    # Step 1: Define the personalities that map to the magi_models list
    # This keeps configuration within the orchestrator script.
    magi_personalities = {
        "balthazar": "Magi Balthazar-1: Polkadot must win.",
        "melchior": "Magi Melchior-2: Polkadot must thrive.",
        "caspar": "Magi Caspar-3: Polkadot must outlive us all.",
    }
    # Create the list of full personality strings to pass to the inference engine
    personalities_to_run = {
        model: magi_personalities[model]
        for model in magi_models if model in magi_personalities
    }


    # Step 2: Read the proposal content downloaded by pre-flight checks
    proposal_content_path = local_workspace / "content.md"
    if not proposal_content_path.exists():
        raise FileNotFoundError(f"Proposal content not found at {proposal_content_path}")
    proposal_text = proposal_content_path.read_text()


    # Step 3: Setup the agent and run inference
    logger.info("  Setting up compiled DSPy agent...")
    compiled_agent, model_name_used = setup_compiled_agent()
    logger.info(f"  Agent compiled. Using model: {model_name_used}")

    logger.info("  Running inference for all personalities...")
    llm_results = run_inference_for_proposal(compiled_agent, personalities_to_run, proposal_text)


    # Step 4: Write the results to JSON files, matching the original format
    output_files = []
    for model_key, result_data in llm_results.items():
        output_path = analysis_dir / f"{model_key}.json"
        data = {
            "model_name": model_name_used,
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "decision": result_data["decision"],
            "confidence": None,
            "rationale": result_data["rationale"],
            "raw_api_response": {} # TODO requires advanced logging in dspy
        }
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        output_files.append(output_path)
        logger.info(f"✅ Generated REAL analysis for {model_key}.")

    return output_files


def consolidate_vote(analysis_files, local_workspace):
    """
    Reads individual LLM analyses and creates a final vote.json file.
    """
    logger.info("03 - Consolidating vote...")
    votes_breakdown = []
    decisions = []
    for analysis_file in analysis_files:
        with open(analysis_file, 'r') as f:
            data = json.load(f)
        model_name = analysis_file.stem
        votes_breakdown.append({
            "model": model_name,
            "decision": data["decision"],
            "confidence": data["confidence"]
        })
        decisions.append(data["decision"])
    
    # Determine final decision
    decision_counts = Counter(decisions)
    final_decision = "ABSTAIN"
    is_conclusive = False
    if decision_counts:
        most_common = decision_counts.most_common(1)
        # Check for a clear majority, otherwise abstain
        if len(decision_counts) == 1 or decision_counts.most_common(2)[0][1] > decision_counts.most_common(2)[1][1]:
            final_decision = most_common[0][0]
            is_conclusive = True
            
    is_unanimous = len(set(decisions)) == 1 if decisions else False
    
    vote_data = {
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "is_conclusive": is_conclusive,
        "final_decision": final_decision,
        "is_unanimous": is_unanimous,
        "summary_rationale": generate_summary_rationale(votes_breakdown),
        "votes_breakdown": votes_breakdown
    }
    
    vote_path = local_workspace / "vote.json"
    with open(vote_path, 'w') as f:
        json.dump(vote_data, f, indent=2)
    logger.info(f"✅ Vote consolidated into {vote_path}.")
    return vote_path


def main():
    logger = setup_logging()
    logger.info("CyberGov V0 ... initializing.")
    last_good_step = "initializing"
    
    try:
        config = get_config_from_env()
        proposal_id = config["PROPOSAL_ID"]
        network = config["NETWORK"]
        s3_bucket = config["S3_BUCKET_NAME"]
        
        s3 = s3fs.S3FileSystem(
            key=config["S3_ACCESS_KEY_ID"],
            secret=config["S3_ACCESS_KEY_SECRET"],
            client_kwargs={"endpoint_url": config["S3_ENDPOINT_URL"]},
            asynchronous=False,
            loop=None  # <-- This is the key addition
        )
        
        proposal_s3_path = f"{s3_bucket}/proposals/{network}/{proposal_id}"
        logger.info(f"Working with S3 path: {proposal_s3_path}")
        
        # Create a local workspace for processing
        local_workspace = Path("workspace")
        local_workspace.mkdir(exist_ok=True)
        
        last_good_step = "s3_and_workspace_setup"

        # Step 1: Check inputs, download them, and record their hashes for the manifest
        manifest_inputs, _, magi_models = perform_preflight_checks(
            s3, proposal_s3_path, local_workspace
        )
        last_good_step = "pre-flight_checks"

        # Step 2: Run evaluations (dummy version)
        local_analysis_files = run_magi_evaluations(magi_models, local_workspace)
        last_good_step = "magi_evaluation"

        # Step 3: Consolidate the vote
        local_vote_file = consolidate_vote(local_analysis_files, local_workspace)
        last_good_step = "vote_consolidation"

        # Step 4: Generate manifest and upload all outputs
        logger.info("04 - Attesting, signing, and uploading outputs...")
        manifest_outputs = []
        files_to_process = local_analysis_files + [local_vote_file]
        
        for local_file in files_to_process:
            file_hash = hash_file(local_file)
            
            # Determine S3 path
            if local_file.parent.name == "llm_analyses":
                s3_filename = f"llm_analyses/{local_file.stem}.json"
            else:
                s3_filename = f"{local_file.stem}.json"
            
            final_s3_path = f"{proposal_s3_path}/{s3_filename}"
            
            # Upload the file
            s3.upload(str(local_file), final_s3_path)
            logger.info(f"  📤 Uploaded {local_file.name} to {final_s3_path}")
            
            # Add entry to manifest outputs
            manifest_outputs.append({
                "logical_name": local_file.stem,
                "s3_path": final_s3_path,
                "hash": file_hash
            })

        # Build the final manifest
        manifest = {
            "provenance": {
                "job_name": "LLM Inference and Voting",
                "github_repository": os.getenv("GITHUB_REPOSITORY", "N/A"),
                "github_run_id": os.getenv("GITHUB_RUN_ID", "N/A"),
                "github_commit_sha": os.getenv("GITHUB_SHA", "N/A"),
                "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
            },
            "inputs": manifest_inputs,
            "outputs": manifest_outputs
        }
        
        manifest_path = local_workspace / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        # Upload manifest and signature with stable names
        s3.upload(str(manifest_path), f"{proposal_s3_path}/manifest.json")
        logger.info("✅ Uploaded manifest.")
        last_good_step = "attestation_and_upload"

        logger.info("🎉 CyberGov V0 processing complete!")

    except Exception as e:
        logger.error(f"\n💥 FATAL ERROR during processing: {e}")
        logger.error(f"Last successful step was: '{last_good_step}'")
        sys.exit(1)


if __name__ == "__main__":
    main()