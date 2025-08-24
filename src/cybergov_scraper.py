from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
import httpx

# --- Configuration Constants ---
GITHUB_REPO = "your-username/your-repo-name"  # <-- IMPORTANT: Change this
WORKFLOW_FILE_NAME = "cybergov.yml"           # The name of your GHA workflow file

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
    
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE_NAME}/dispatches"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {github_pat}",
    }
    
    # The payload now includes both proposal_id and network
    data = {
        "ref": "main", # Or your default branch
        "inputs": {
            "proposal_id": str(proposal_id),
            "network": network,
        }
    }
    
    with httpx.Client() as client:
        response = client.post(url, headers=headers, json=data)
        
    if response.status_code == 204:
        logger.info(f"Successfully triggered GitHub Action for proposal ID: {proposal_id}")
    else:
        logger.error(f"Failed to trigger GitHub Action. Status: {response.status_code}, Body: {response.text}")
        # Fail the task so the flow run is marked as Failed.
        response.raise_for_status()

@flow(name="Proposal Scraper", log_prints=True)
def proposal_scraper_flow(proposal_id: int, network: str):
    """
    A simple, parameterized flow that is scheduled by the dispatcher.
    Its sole purpose is to trigger the GitHub Action worker for a specific proposal.
    """
    trigger_github_action_worker(proposal_id=proposal_id, network=network)

if __name__ == "__main__":
    # Deploy this flow. It has no schedule and will only run when called by the dispatcher.
    proposal_scraper_flow.serve(
        name="proposal-scraper-deployment"
    )