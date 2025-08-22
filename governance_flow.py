import random
import time
from collections import Counter
from typing import List, Dict

from prefect import flow, task, get_run_logger

# --- Task Definitions ---
# Tasks are the building blocks of your workflow. They are just Python functions
# with a @task decorator.

@task
def fetch_new_proposals() -> List[Dict]:
    """Simulates fetching active governance proposals."""
    logger = get_run_logger()
    logger.info("Fetching new governance proposals...")
    # In reality, this would be an API call to a Polkadot node or Subscan.
    return [
        {"id": 101, "title": "Treasury Proposal: Fund Project X"},
        {"id": 102, "title": "Runtime Upgrade: v0.9.42"},
    ]

@task
def analyze_with_llm_alpha(proposal: Dict) -> str:
    """Simulates a reliable LLM analyzing a proposal."""
    logger = get_run_logger()
    vote = random.choice(["YEA", "NAY", "ABSTAIN"])
    logger.info(f"LLM Alpha analyzed proposal {proposal['id']}: Voted {vote}")
    time.sleep(2) # Simulate work
    return vote

@task(retries=3, retry_delay_seconds=5)
def analyze_with_flaky_llm_beta(proposal: Dict) -> str:
    """
    Simulates an UNRELIABLE LLM. It will fail 60% of the time.
    ADVANTAGE DEMO: Prefect will automatically retry this task on failure.
    """
    logger = get_run_logger()
    if random.random() < 0.6:
        logger.error(f"LLM Beta FAILED to analyze proposal {proposal['id']}! Simulating API error.")
        raise ValueError("Simulated API Error")
    
    vote = random.choice(["YEA", "NAY"])
    logger.info(f"LLM Beta analyzed proposal {proposal['id']}: Voted {vote}")
    time.sleep(3) # Simulate more work
    return vote

@task
def aggregate_votes(votes: List[str]) -> str:
    """Determines the final consensus vote."""
    logger = get_run_logger()
    logger.info(f"Aggregating votes: {votes}")
    vote_counts = Counter(votes)
    # Get the most common vote, if there is one and it's not ABSTAIN
    most_common = vote_counts.most_common(1)[0]
    if most_common[0] != "ABSTAIN" and most_common[1] > 1:
        final_decision = most_common[0]
        logger.info(f"Consensus reached: {final_decision}")
        return final_decision
    else:
        logger.warning("No clear consensus. Will not vote.")
        return "NO_CONSENSUS"

@task
def submit_on_chain_vote(proposal_id: int, decision: str):
    """Simulates signing and submitting the final vote transaction."""
    logger = get_run_logger()
    if decision != "NO_CONSENSUS":
        logger.info(f"✅ SUBMITTING ON-CHAIN VOTE: {decision} for proposal {proposal_id}")
        # In reality, this is where you'd use your private key.
    else:
        logger.info(f"ℹ️ SKIPPING VOTE for proposal {proposal_id} due to no consensus.")


# --- Flow Definition ---
# A flow is the main container for your workflow logic.

@flow(name="Polkadot Governance Voting")
def governance_flow():
    """
    The main flow to fetch proposals, analyze them with LLMs, and vote.
    """
    logger = get_run_logger()
    logger.info("Starting governance flow...")
    
    proposals = fetch_new_proposals()

    for proposal in proposals:
        logger.info(f"--- Processing Proposal ID: {proposal['id']} ---")
        
        # ADVANTAGE DEMO: Parallel Execution
        # By using .submit(), we tell Prefect to run these tasks concurrently,
        # not one after the other. This is a massive time-saver.
        alpha_vote_future = analyze_with_llm_alpha.submit(proposal)
        beta_vote_future = analyze_with_flaky_llm_beta.submit(proposal)
        
        # Gather the results. Prefect waits here until both tasks are complete.
        all_votes = [
            alpha_vote_future.result(),
            beta_vote_future.result()
        ]
        
        final_decision = aggregate_votes(all_votes)
        submit_on_chain_vote(proposal['id'], final_decision)

if __name__ == "__main__":
    governance_flow()
