import sys
import json
import datetime
import s3fs
import os 

from utils.helpers import setup_logging, get_config_from_env, hash_file
from utils.run_magi_eval import run_single_inference, setup_compiled_agent
from pathlib import Path
from collections import Counter

logger = setup_logging()


def generate_summary_rationale(votes_breakdown, proposal_id, network, analysis_files) -> str:
    """
    Placeholder for the LLM call to generate a summary rationale.
    """
    logger.info("--> Generatign simple concatenated rationale...")
    # This would be a real LLM call in production.
    aye_votes = sum(1 for v in votes_breakdown if v['decision'].upper() == 'AYE')
    nay_votes = sum(1 for v in votes_breakdown if v['decision'].upper() == 'NAY')
    abstain_votes = sum(1 for v in votes_breakdown if v['decision'].upper() == 'ABSTAIN')

    balthazar_rationale, balthazar_decision = None, None
    melchior_rationale, melchior_decision = None, None
    caspar_rationale, caspar_decision = None, None

    for analysis_file in analysis_files:
        with open(analysis_file, 'r') as f:
            data = json.load(f)
        
        # Use the .name attribute of the Path object for comparison
        if analysis_file.name == 'balthazar.json':
            balthazar_rationale = data['rationale']
            balthazar_decision = data['decision']
        elif analysis_file.name == 'melchior.json':
            melchior_rationale = data['rationale']
            melchior_decision = data['decision']
        elif analysis_file.name == 'caspar.json':
            caspar_rationale = data['rationale']
            caspar_decision = data['decision']

    ## TODO get the vote number in here to inform people that this might not be the first vote (old links will go stale)
    # requires a way to edit old proposal comments, maybe for later
    summary_text = f"""
<h2>CYBERGOV V0</h2>
<p>The models voted {aye_votes} AYE, {nay_votes} NAY and {abstain_votes} ABSTAIN.</p>
<h2>Outcome</h2>
<ul>
    <li>Balthazar voted <a href="https://cybergov.b-cdn.net/proposals/{network}/{proposal_id}/llm_analyses/balthazar.json">{balthazar_decision}</a></li>
    <li>Melchior voted <a href="https://cybergov.b-cdn.net/proposals/{network}/{proposal_id}/llm_analyses/melchior.json">{melchior_decision}</a></li>
    <li>Caspar voted <a href="https://cybergov.b-cdn.net/proposals/{network}/{proposal_id}/llm_analyses/balthazar.json">{balthazar_decision}</a></li>
</ul>
<p>In case of questions, remarks or contributions, please refer to:</p>
<ul>
    <li><a href="https://github.com/KarimJedda/cybergov/issues">https://github.com/KarimJedda/cybergov/issues</a></li>
</ul>
"""

    return summary_text


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


def run_magi_evaluations(magi_models_list, local_workspace):
    """
    Runs LLM evaluations by compiling a separate, optimized agent for each Magi's model.
    The function signature remains unchanged.
    """
    logger.info("02 - Running MAGI V0 Evaluation (Compile-per-Model strategy)...")
    analysis_dir = local_workspace / "llm_analyses"
    analysis_dir.mkdir(exist_ok=True)

    # --- Full configuration for all available Magi ---
    magi_personalities = {
        "balthazar": "Magi Balthazar-1: Polkadot must win. Consider all proposals through the lens of strategic advantage and competitive positioning.",
        "melchior": "Magi Melchior-2: Polkadot must thrive. Focus on ecosystem growth, developer activity, and user adoption.",
        "caspar": "Magi Caspar-3: Polkadot must outlive us all. Prioritize long-term sustainability, sound economic models, and protocol resilience over short-term gains.",
    }

    magi_llms = {
        "balthazar": "openai/gpt-4o",
        "melchior": "openrouter/google/gemini-2.5-pro-preview",
        "caspar": "openrouter/x-ai/grok-code-fast-1",
    }


    proposal_content_path = local_workspace / "content.md"
    if not proposal_content_path.exists():
        raise FileNotFoundError(f"Proposal content not found at {proposal_content_path}")
    proposal_text = proposal_content_path.read_text()
    
    output_files = []
    for magi_key in magi_models_list:
        if magi_key not in magi_llms:
            logger.warning(f"Skipping '{magi_key}': No model configured.")
            continue

        model_id = magi_llms[magi_key]
        personality_prompt = magi_personalities[magi_key]

        logger.info(f"--- Processing Magi: {magi_key.upper()} ---")

        # Step A: Compile a new agent specifically for this model
        logger.info(f"  Compiling agent using model: {model_id}...")
        compiled_agent = setup_compiled_agent(model_id=model_id)

        # Step B: Run a single inference with the newly compiled agent
        logger.info(f"  Running inference for {magi_key}...")
        prediction = run_single_inference(compiled_agent, personality_prompt, proposal_text)

        # Step C: Write the result to a JSON file
        output_path = analysis_dir / f"{magi_key}.json"
        data = {
            "model_name": model_id,
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "decision": prediction.vote.strip(),
            "confidence": None,
            "rationale": prediction.rationale.strip(),
            "raw_api_response": {}
        }
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        output_files.append(output_path)
        logger.info(f"✅ Generated analysis for {magi_key} and saved to {output_path.name}")

    return output_files


def consolidate_vote(analysis_files, local_workspace, proposal_id, network):
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
        "summary_rationale": generate_summary_rationale(votes_breakdown, proposal_id, network, analysis_files),
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
        local_vote_file = consolidate_vote(local_analysis_files, local_workspace, proposal_id, network)
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