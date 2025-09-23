import sys
import json
import datetime
import s3fs
import os
import hashlib

from utils.helpers import setup_logging, get_config_from_env, hash_file
from utils.run_magi_eval import run_single_inference, setup_compiled_agent
from pathlib import Path
from collections import Counter

logger = setup_logging()


def load_magi_personalities() -> dict[str, str]:
    """
    Load Magi personalities from system prompt files.
    
    Returns:
        dict: Dictionary mapping magi names to their personality descriptions
    """
    personalities = {}
    system_prompts_dir = Path(__file__).parent.parent / "templates" / "system_prompts"
    
    magi_names = ["balthazar", "melchior", "caspar"]
    
    for magi_name in magi_names:
        prompt_file = system_prompts_dir / f"{magi_name}_system_prompt.md"
        try:
            if prompt_file.exists():
                with open(prompt_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    personalities[magi_name] = content
                logger.info(f"✅ Loaded {magi_name} personality from {prompt_file}")
            else:
                logger.warning(f"⚠️ System prompt file not found: {prompt_file}")
        except Exception as e:
            logger.error(f"❌ Error loading {magi_name} personality: {e}")
    
    return personalities


def generate_summary_rationale(
    votes_breakdown, proposal_id, network, analysis_files
) -> str:
    """
    Placeholder for the LLM call to generate a summary rationale.
    """
    logger.info("--> Generatign simple concatenated rationale...")
    github_run_id = os.getenv("GITHUB_RUN_ID", "N/A")
    aye_votes = sum(1 for v in votes_breakdown if v["decision"].upper() == "AYE")
    nay_votes = sum(1 for v in votes_breakdown if v["decision"].upper() == "NAY")
    abstain_votes = sum(
        1 for v in votes_breakdown if v["decision"].upper() == "ABSTAIN"
    )

    balthazar_rationale, balthazar_decision = None, None
    melchior_rationale, melchior_decision = None, None
    caspar_rationale, caspar_decision = None, None

    for analysis_file in analysis_files:
        with open(analysis_file, "r") as f:
            data = json.load(f)

        # Use the .name attribute of the Path object for comparison
        if analysis_file.name == "balthazar.json":
            balthazar_rationale = data['rationale']
            balthazar_decision = data["decision"]
        elif analysis_file.name == "melchior.json":
            melchior_rationale = data['rationale']
            melchior_decision = data["decision"]
        elif analysis_file.name == "caspar.json":
            caspar_rationale = data['rationale']
            caspar_decision = data["decision"]

    ## TODO get the vote number in here to inform people that this might not be the first vote (old links will go stale)
    # requires a way to edit old proposal comments, maybe for later
    summary_text = f"""
<p>A panel of autonomous agents reviewed this proposal, resulting in a vote of <strong>{aye_votes} AYE</strong>, <strong>{nay_votes} NAY</strong>, and <strong>{abstain_votes} ABSTAIN</strong>.</p>
<h3 style="display: inline;">Balthazar voted <u>{balthazar_decision}</u></h3>
<blockquote>{balthazar_rationale}</blockquote>
<h3 style="display: inline;">Melchior voted <u>{melchior_decision}</u></h3>
<blockquote>{melchior_rationale}</blockquote>
<h3 style="display: inline;">Caspar voted <u>{caspar_decision}</u></h3>
<blockquote>{caspar_rationale}</blockquote>
<h3>Feedback</h3>
<p>Help improve the system by letting us know if the analysis was helpful:</p>
<ul>
    <li><a href="https://docs.google.com/forms/d/e/1FAIpQLSdEvZEUzccs58Ez49l0RSJnuRFed2wR_QstxbrJLbOosndowg/viewform?usp=pp_url&entry.799132028=https://{network}.subsquare.io/referenda/{proposal_id}&entry.1205216491=Agree+with+the+vote&entry.1493217809=Just+right" target="_blank">👍 Helpful</a></li>
    <li><a href="https://docs.google.com/forms/d/e/1FAIpQLSdEvZEUzccs58Ez49l0RSJnuRFed2wR_QstxbrJLbOosndowg/viewform?usp=pp_url&entry.799132028=https://{network}.subsquare.io/referenda/{proposal_id}&entry.1205216491=Disagree+with+the+vote&entry.1493217809=One+of+the+LLMs+goofed+completely" target="_blank">👎 Unhelpful</a></li>
</ul>
<h3>System Transparency</h3>
<p>To ensure full transparency, all data and processes related to this vote are publicly available:</p>
<ul>
    <li><strong>Manifest File:</strong> <a href="https://cybergov.b-cdn.net/proposals/{network}/{proposal_id}/manifest.json">View the full inputs and outputs.</a></li>
    <li><strong>Execution Log:</strong> <a href="https://github.com/KarimJedda/cybergov/actions/runs/{github_run_id}">Verify the public GitHub pipeline run and compare the manifest.json hash.</a></li>
    <li><strong>Source Content:</strong> <a href="https://cybergov.b-cdn.net/proposals/{network}/{proposal_id}/content.md">Read the content provided to the agents.</a></li>
    <li><strong>Read about how this works:</strong> <a href="https://forum.polkadot.network/t/cybergov-v0-automating-trust-verifiable-llm-governance-on-polkadot/14796">Technical write-up.</a></li>
    <li><strong>Request a re-vote:</strong> <a href="https://github.com/KarimJedda/cybergov/issues">Cybergov public issue tracker.</a></li>
</ul>
<hr>
<h3>A Note on This System</h3>
<p>Please be aware that this analysis was produced by Large Language Models (LLMs). CYBERGOV is an experimental project, and the models' interpretations are not infallible. They can make mistakes or overlook nuance. They also <strong>currently lack historical context</strong>, work is underway to extend CYBERGOV with embeddings and more. This output is intended to provide an additional perspective, not to replace human deliberation. We encourage community feedback to help improve the system.</p>
<p>Further details on the project are available at the <a href="https://github.com/KarimJedda/cybergov">main repository</a>. Consider delegating to CYBERGOV :)</p>
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
        logger.error("Something went wrong finding raw_subsquare_data.json")
        sys.exit(1)

    with s3.open(raw_subsquare_s3_path, "r") as f:
        raw_data = json.load(f)
    required_attrs = ["referendumIndex", "title", "content", "proposer"]
    if not all(attr in raw_data for attr in required_attrs):
        logger.error("Something went wrong validating raw_subsquare.json")
        sys.exit(1)

    # Download, hash, and record raw_subsquare.json
    local_raw_path = local_workspace / "raw_subsquare.json"
    try:
        s3.download(raw_subsquare_s3_path, str(local_raw_path))
    except Exception as e:
        logger.error("Something went wrong downloading raw_subsquare.json")
        sys.exit(1)
    manifest_inputs.append(
        {
            "logical_name": "raw_subsquare_data",
            "s3_path": raw_subsquare_s3_path,
            "hash": hash_file(local_raw_path),
        }
    )
    logger.info("✅ raw_subsquare.json found and validated.")

    # 2. Check for content.md in S3
    content_md_s3_path = f"{proposal_s3_path}/content.md"
    if not s3.exists(content_md_s3_path):
        logger.error("Something went wrong finding content.md")
        sys.exit(1)

    # Download, hash, and record content.md
    local_content_path = local_workspace / Path(content_md_s3_path).name
    try:
        s3.download(content_md_s3_path, str(local_content_path))
    except Exception as e:
        logger.error("Something went wrong downloading content.md")
        sys.exit(1)
    manifest_inputs.append(
        {
            "logical_name": "content_markdown",
            "s3_path": content_md_s3_path,
            "hash": hash_file(local_content_path),
        }
    )
    logger.info(f"✅ {Path(content_md_s3_path).name} found.")

    # Currently useless, see run_magi_evaluations.magi_personalities
    prompt_dir = Path("templates/system_prompts")
    magi_models = ["balthazar", "caspar", "melchior"]
    for model in magi_models:
        prompt_file = prompt_dir / f"{model}_system_prompt.md"
        if not prompt_file.exists():
            logger.error(f"Something went wrong finding {model}_system_prompt.md")
            sys.exit(1)
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

    # Load personalities from system prompt files
    magi_personalities = load_magi_personalities()

    # TODO maybe pick from a random list?
    magi_llms = {
        "balthazar": "openai/gpt-4o",
        "melchior": "openrouter/google/gemini-2.5-pro-preview",
        "caspar": "openrouter/x-ai/grok-code-fast-1",
    }

    proposal_content_path = local_workspace / "content.md"
    if not proposal_content_path.exists():
        logger.error("Something went wrong finding proposal content")
        sys.exit(1)
    proposal_text = proposal_content_path.read_text()

    output_files = []
    for magi_key in magi_models_list:
        if magi_key not in magi_llms:
            logger.warning(f"Skipping '{magi_key}': No model configured.")
            continue

        model_id = magi_llms[magi_key]
        personality_prompt = magi_personalities[magi_key]

        logger.info(f"--- Processing Magi: {magi_key.upper()} ---")

        # Step A: Compile a new agent specifically for this model, maybe we will need this compiled by the same LLM? idk
        logger.info(f"  Compiling agent using model: {model_id}...")
        compiled_agent = setup_compiled_agent(model_id=model_id)

        # Step B: Run a single inference with the newly compiled agent
        logger.info(f"  Running inference for {magi_key}...")
        prediction = run_single_inference(
            compiled_agent, personality_prompt, proposal_text
        )

        # Step C: Write the result to a JSON file
        output_path = analysis_dir / f"{magi_key}.json"
        data = {
            "model_name": model_id,
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "decision": prediction.vote.strip(),
            "confidence": None,
            "rationale": prediction.rationale.strip(),
            "raw_api_response": {},
        }
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)

        output_files.append(output_path)
        logger.info(
            f"✅ Generated analysis for {magi_key} and saved to {output_path.name}"
        )

    return output_files


def consolidate_vote(analysis_files, local_workspace, proposal_id, network):
    """
    Reads individual LLM analyses and creates a final vote.json file.
    """
    logger.info("03 - Consolidating vote...")
    votes_breakdown = []
    decisions = []
    
    for analysis_file in analysis_files:
        with open(analysis_file, "r") as f:
            data = json.load(f)
        model_name = analysis_file.stem
        
        decision = data["decision"].strip()
        if decision.upper() == "AYE":
            normalized_decision = "Aye"
        elif decision.upper() == "NAY":
            normalized_decision = "Nay"
        else:  # ABSTAIN or any other value
            normalized_decision = "Abstain"
        
        votes_breakdown.append(
            {
                "model": model_name,
                "decision": normalized_decision,
                "confidence": data["confidence"],
            }
        )
        decisions.append(normalized_decision)

    if not decisions:
        final_decision = "Abstain"
        is_conclusive = False
        is_unanimous = False
    else:
        is_unanimous = len(set(decisions)) == 1

        if is_unanimous:
            # If all votes are the same, that's our final decision.
            final_decision = decisions[0]
            # A conclusive vote is only cast if there is unanimity.
            # TODO: make use of the conclusive variable or throw it out, it is redundant
            is_conclusive = True
        else:
            # Apply decision table logic:
            # - Two Aye and one Abstain -> Aye
            # - Two Nay and one Abstain -> Nay
            # - Any other disagreement -> Abstain
            counts = Counter(decisions)
            aye = counts.get("Aye", 0)
            nay = counts.get("Nay", 0)
            abstain = counts.get("Abstain", 0)

            if aye == 2 and nay == 0:
                final_decision = "Aye"
                is_conclusive = False
            elif nay == 2 and aye == 0:
                final_decision = "Nay"
                is_conclusive = False
            else:
                final_decision = "Abstain"
                is_conclusive = False

    vote_data = {
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "is_conclusive": is_conclusive,
        "final_decision": final_decision,
        "is_unanimous": is_unanimous,
        "summary_rationale": generate_summary_rationale(
            votes_breakdown, proposal_id, network, analysis_files
        ),
        "votes_breakdown": votes_breakdown,
    }

    vote_path = local_workspace / "vote.json"
    with open(vote_path, "w") as f:
        json.dump(vote_data, f, indent=2)
    logger.info(f"✅ Vote consolidated into {vote_path}.")
    return vote_path


def setup_s3_and_workspace(config):
    """
    Initialize S3 filesystem and create local workspace.
    Returns S3 filesystem, proposal S3 path, and local workspace path.
    """
    proposal_id = config["PROPOSAL_ID"]
    network = config["NETWORK"]
    s3_bucket = config["S3_BUCKET_NAME"]

    try:
        # Make sure we prevent credential logging
        s3 = s3fs.S3FileSystem(
            key=config["S3_ACCESS_KEY_ID"],
            secret=config["S3_ACCESS_KEY_SECRET"],
            client_kwargs={"endpoint_url": config["S3_ENDPOINT_URL"]},
            asynchronous=False,
            loop=None,
        )
        s3.ls(s3_bucket, detail=False, refresh=True) # Test connection
    except Exception as e:
        logger.error("Something went wrong during S3 initialization")
        sys.exit(1)

    proposal_s3_path = f"{s3_bucket}/proposals/{network}/{proposal_id}"
    logger.info(f"Working with S3 path: {proposal_s3_path}")

    # Create a local workspace for processing
    local_workspace = Path("workspace")
    local_workspace.mkdir(exist_ok=True)
    
    return s3, proposal_s3_path, local_workspace, proposal_id, network


def upload_outputs_and_generate_manifest(s3, proposal_s3_path, local_workspace, local_analysis_files, local_vote_file, manifest_inputs):
    """
    Upload output files to S3 and generate the final manifest.
    Returns the manifest data structure.
    """
    logger.info("04 - Attesting, signing, and uploading outputs...")
    manifest_outputs = []
    files_to_process = local_analysis_files + [local_vote_file]

    for local_file in files_to_process:
        file_hash = hash_file(local_file)

        if local_file.parent.name == "llm_analyses":
            s3_filename = f"llm_analyses/{local_file.stem}.json"
        else:
            s3_filename = f"{local_file.stem}.json"

        final_s3_path = f"{proposal_s3_path}/{s3_filename}"

        try:
            s3.upload(str(local_file), final_s3_path)
            logger.info(f"  📤 Uploaded {local_file.name}")
        except Exception as e:
            logger.error(f"Something went wrong uploading {local_file.name}")
            sys.exit(1)

        manifest_outputs.append(
            {
                "logical_name": local_file.stem,
                "s3_path": final_s3_path,
                "hash": file_hash,
            }
        )

    # Build the final manifest
    manifest = {
        "provenance": {
            "job_name": "LLM Inference and Voting",
            "github_repository": os.getenv("GITHUB_REPOSITORY", "N/A"),
            "github_run_id": os.getenv("GITHUB_RUN_ID", "N/A"),
            "github_commit_sha": os.getenv("GITHUB_SHA", "N/A"),
            "timestamp_utc": datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat(),
        },
        "inputs": manifest_inputs,
        "outputs": manifest_outputs,
    }

    logger.info(f"Manifest outputs: {manifest['outputs']}")
    
    canonical_manifest = json.dumps(
        manifest, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    canonical_manifest_sha256 = hashlib.sha256(canonical_manifest).hexdigest()
    logger.info(f"Canonical SHA256 of the manifest: {canonical_manifest_sha256}")

    manifest_path = local_workspace / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    try:
        s3.upload(str(manifest_path), f"{proposal_s3_path}/manifest.json")
        logger.info("✅ Uploaded manifest.")
    except Exception as e:
        logger.error("Something went wrong uploading manifest")
        sys.exit(1)
    
    return manifest


def main():
    logger = setup_logging()
    logger.info("CyberGov V0 ... initializing.")
    last_good_step = "initializing"

    try:
        config = get_config_from_env()
        
        s3, proposal_s3_path, local_workspace, proposal_id, network = setup_s3_and_workspace(config)
        last_good_step = "s3_and_workspace_setup"

        manifest_inputs, _, magi_models = perform_preflight_checks(
            s3, proposal_s3_path, local_workspace
        )
        last_good_step = "pre-flight_checks"

        local_analysis_files = run_magi_evaluations(magi_models, local_workspace)
        last_good_step = "magi_evaluation"

        local_vote_file = consolidate_vote(
            local_analysis_files, local_workspace, proposal_id, network
        )
        last_good_step = "vote_consolidation"

        upload_outputs_and_generate_manifest(
            s3, proposal_s3_path, local_workspace, local_analysis_files, local_vote_file, manifest_inputs
        )
        last_good_step = "attestation_and_upload"

        logger.info("🎉 CyberGov V0 processing complete!")

    except Exception as e:
        logger.error("\n💥 Something went wrong during vote evaluation")
        logger.error(f"Last successful step was: '{last_good_step}'")
        sys.exit(1)


if __name__ == "__main__":
    main()
