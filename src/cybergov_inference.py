from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
import httpx

# --- Configuration Constants ---
GITHUB_REPO = "KarimJedda/cybergov"

NETWORK_MAPPING = {
    'polkadot': 'run_polkadot.yml',
    'kusama': 'run_kusama.yml',
    'paseo': 'run_paseo.yml'
}

@task
def trigger_github_action_worker(proposal_id: int, network: str):
    """
    Makes an API call to GitHub to trigger the `workflow_dispatch` event,
    passing the proposal ID and network as inputs.
    """
    logger = get_run_logger()
    logger.info(f"Triggering GitHub Action for proposal {proposal_id} on network '{network}'")
    
    try:
        github_pat = Secret.load("github-pat").get()
    except ValueError:
        logger.error("Could not load 'github-pat' Secret block from Prefect.")
        raise

    # TODO has to fail if provided bad values
    WORKFLOW_FILE_NAME = NETWORK_MAPPING[network]
    
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE_NAME}/dispatches"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {github_pat}",
    }
    
    data = {
        "ref": "main",
        "inputs": {
            "proposal_id": str(proposal_id)
        }
    }
    
    with httpx.Client() as client:
        response = client.post(url, headers=headers, json=data)
        
    if response.status_code == 204:
        logger.info(f"Successfully triggered GitHub Action for proposal ID: {proposal_id}")
    else:
        logger.error(f"Failed to trigger GitHub Action. Status: {response.status_code}, Body: {response.text}")
        response.raise_for_status()

@flow(name="GitHub Action Trigger", log_prints=True)
def github_action_trigger(proposal_id: int, network: str):
    """
    A simple, parameterized flow that is scheduled by the dispatcher.
    Its sole purpose is to trigger the GitHub Action worker for a specific proposal.
    """
    trigger_github_action_worker(proposal_id=proposal_id, network=network)
