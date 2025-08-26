from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
import httpx
from datetime import datetime, timedelta, timezone
import time 

# To ensure transparency, this has to run on GitHub actions
# That way it is public, and the data + logic used to vote are transparent
GITHUB_REPO = "KarimJedda/cybergov"

NETWORK_MAPPING = {
    'polkadot': 'run_polkadot.yml',
    'kusama': 'run_kusama.yml',
    'paseo': 'run_paseo.yml'
}

POLL_INTERVAL_SECONDS = 15
FIND_RUN_TIMEOUT_SECONDS = 300 
POLL_STATUS_TIMEOUT_SECONDS = 700

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
    workflow_file_name = NETWORK_MAPPING[network]
    
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{workflow_file_name}/dispatches"
    
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

    trigger_time = datetime.now(timezone.utc)
    
    with httpx.Client() as client:
        response = client.post(url, headers=headers, json=data)
        
    if response.status_code == 204:
        logger.info(f"Successfully triggered GitHub Action for proposal ID: {proposal_id}")
        return workflow_file_name, trigger_time
    else:
        logger.error(f"Failed to trigger GitHub Action. Status: {response.status_code}, Body: {response.text}")
        response.raise_for_status()

@task
def find_workflow_run(network: str, proposal_id: int, workflow_file_name: str, trigger_time: datetime):
    """
    Finds the specific workflow run that was triggered after a given timestamp.
    """
    logger = get_run_logger()
    logger.info(f"Searching for new workflow run for '{workflow_file_name}'...")

    github_pat = Secret.load("github-pat").get()
    # API https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2022-11-28#list-workflow-runs-for-a-workflow
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{workflow_file_name}/runs"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {github_pat}",
    }
    params = {"event": "workflow_dispatch", "branch": "main", "per_page": 5}

    start_time = datetime.now(timezone.utc)
    while datetime.now(timezone.utc) - start_time < timedelta(seconds=FIND_RUN_TIMEOUT_SECONDS):
        with httpx.Client() as client:
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            runs = response.json().get("workflow_runs", [])

        for run in runs:
            # GitHub's created_at is a string like '2023-10-27T10:00:00Z'
            created_at = datetime.fromisoformat(run['created_at'].replace('Z', '+00:00'))
            display_title = run['display_title'].lower()
            if created_at >= trigger_time and f"#{proposal_id} on {network}" in display_title:
                logger.info(f"Found matching workflow run with ID: {run['id']}")
                return run['id']

        logger.info("No matching run found yet. Waiting...")
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError("Timed out waiting to find the triggered workflow run.")

@task
def poll_workflow_run_status(run_id: int):
    """
    Polls the status of a specific workflow run until it completes.
    Raises an exception if the run fails.
    """
    logger = get_run_logger()
    logger.info(f"Polling status for workflow run ID: {run_id}")

    github_pat = Secret.load("github-pat").get()
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/runs/{run_id}"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {github_pat}",
    }
    
    start_time = datetime.now(timezone.utc)
    while datetime.now(timezone.utc) - start_time < timedelta(seconds=POLL_STATUS_TIMEOUT_SECONDS):
        with httpx.Client() as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            run_data = response.json()

        # Docs: https://github.com/orgs/community/discussions/70540
        status = run_data['status']
        conclusion = run_data['conclusion']
        
        logger.info(f"Run {run_id} status is '{status}'.")

        if status == 'completed':
            logger.info(f"Run {run_id} completed with conclusion: '{conclusion}'.")
            if conclusion == 'success':
                return conclusion
            else:
                error_message = f"GitHub Action run {run_id} failed with conclusion: '{conclusion}'."
                logger.error(error_message)
                raise

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(f"Timed out waiting for workflow run {run_id} to complete.")



@flow(name="GitHub Action Trigger and Monitor", log_prints=True)
def github_action_trigger_and_monitor(proposal_id: int, network: str):
    """
    Triggers a GitHub Action, waits for it to complete, and checks its status.
    """
    logger = get_run_logger()

    workflow_file_name, trigger_time = trigger_github_action_worker(
        proposal_id=proposal_id, 
        network=network
    )
    
    run_id = find_workflow_run(
        network=network,
        proposal_id=proposal_id,
        workflow_file_name=workflow_file_name,
        trigger_time=trigger_time,
        wait_for=[trigger_github_action_worker]
    )

    conclusion = poll_workflow_run_status(
        run_id=run_id,
        wait_for=[find_workflow_run]
    )
    
    if conclusion == 'success':
        logger.info("✅ Magi Inference was successful! Scheduling vote.")