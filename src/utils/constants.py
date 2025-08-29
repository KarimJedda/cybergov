CYBERGOV_PARAMS = {
    ## skip proposals before this id, regardless of what's going on
    'min_proposal_id': {
        'polkadot': 1723,
        'kusama': 578,
        'paseo': 99
    },
    'max_proposal_id': {} # for later
}



DATA_SCRAPER_DEPLOYMENT_ID = "00b42f26-0ccf-4d18-b127-a273b2006838" 
## We wait a little bit before scraping, so people get time to add their links etc. 
SCRAPING_SCHEDULE_DELAY_DAYS = 2


COMMENTING_DEPLOYMENT_ID = "36bdbe3d-82c0-4a80-a7c3-8ee5e485c51c" 
COMMENTING_SCHEDULE_DELAY_MINUTES = 30

GITHUB_REPO = "KarimJedda/cybergov"

GH_WORKFLOW_NETWORK_MAPPING = {
    'polkadot': 'run_polkadot.yml',
    'kusama': 'run_kusama.yml',
    'paseo': 'run_paseo.yml'
}

GH_POLL_INTERVAL_SECONDS = 15
INFERENCE_FIND_RUN_TIMEOUT_SECONDS = 300 
GH_POLL_STATUS_TIMEOUT_SECONDS = 700

VOTING_DEPLOYMENT_ID = "c202dacd-2461-4aac-8ac1-83dd9f27ccc5" 
VOTING_SCHEDULE_DELAY_MINUTES = 30


INFERENCE_TRIGGER_DEPLOYMENT_ID = "327f24eb-04db-4d30-992d-cce455b4b241" 

INFERENCE_SCHEDULE_DELAY_MINUTES = 30

NETWORK_MAP = {
    "polkadot": "https://polkadot-api.subsquare.io/gov2/referendums",
    "kusama": "https://kusama-api.subsquare.io/gov2/referendums",
    "paseo": "https://paseo-api.subsquare.io/gov2/referendums"
}

# Mapping for user-friendly conviction input
CONVICTION_MAPPING = {
    0: "None",
    1: "Locked1x",
    2: "Locked2x",
    3: "Locked3x",
    4: "Locked4x",
    5: "Locked5x",
    6: "Locked6x",
}
