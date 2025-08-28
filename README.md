# cybergov

LLMs participating in governance decisions. Inspired from Evangelion - Magi system. 


## How this works

- The most important step, as usual, is the data. The data collection, sanitization and archival of the data is semi-automated. The heavy lifting is done by a script, but a manual sanitization step is required to make sure no wrong information enters the LLM context. In a separate step, this can be automated but in the interest of a V0, I'm leaving it it out for now.
- When a new proposal is discovered, an init script is manually run that scaffolds the folder and scrapes Subsquare for the key information. This sets the status to "AWAITING_CONTENT_PREP" As most information is linked on third party websites, the URLs of these files are printed out and a further step is required. 
- To solve file risk & content quality, a manual step is required: having a pass over the URLs & files. These are manually downloaded and verified. Then, they are synthesized in a content.md file containing the most important information. This step then sets the status to "AWAITING_LLM_ANALYSIS"
- Every day, an automated pipeline (GitHub + Prefect) is run, which logs the CIDs of all the files used for the decision and casts a vote. All the information for this decision is provided in the logs of GitHub action runners (which aren't self hosted to signal that there was no interference, this obviously mitigates the trust assumption a bit but not completely)
- When everything worked, the metadata.json is set to "VOTED_SUCCESS"

Note: In the event of a bug, or required intervention, we have to document things somehow. An idea would be to provide a correction file to the proposal folder. Like, when a proposal was altered significantly after we voted on it, or something else.



## How the data is stored

In order to ensure reproducibility, everything should ideally be driven of immutable files. Given that we'll work on files, we go with an S3 compatible storage, storing the files like so:

```
s3://your-bucket/proposals/{network}/{proposal_id}/
├── raw_subsquare.json		  # Raw data extracted from Subsquare
├── content.md 				      # Cleaned, extracted content for the LLMs
├── llm_analyses/           # NEW: Directory for individual LLM outputs
│   ├── balthazar.json
│   ├── caspar.json
│   └── melchior.json
├── manifest.json           # hashes of inputs / outputs for provenance
├── manifest.json.sig       # signature of the manifest with the cybergov key
└── vote.json 		          # The final vote result
```

network will be one of polkadot, kusama, paseo. These files will be publicly accessible but served through a CDN. 


## llm_analyses/magi.json

```
{
  "model_name": "claude-3-opus",
  "timestamp_utc": "2023-10-28T13:30:00Z",
  "decision": "AYE",
  "confidence": 0.98,
  "rationale": "The proposal aligns with the stated goal of increasing network security by funding a well-respected audit firm. The budget is reasonable for the scope of work.",
  "raw_api_response": { ... } 
}
```

## vote.json

```
{
  "timestamp_utc": "2023-10-28T13:35:00Z",
  "is_conclusive": true, 
  "final_decision": "AYE",
  "is_unanimous": false,
  "summary_rationale": "The majority decision is AYE. Two models (Claude-3, GPT-4) found the proposal aligned with network security goals and had a reasonable budget. One model (Gemini-1.5) dissented, citing concerns about long-term maintenance costs not being factored in.",
  "votes_breakdown": [
    {
      "model": "balthazar",
      "decision": "AYE",
      "confidence": 0.98
    },
    {
      "model": "melchior",
      "decision": "AYE",
      "confidence": 0.91
    },
    {
      "model": "caspar",
      "decision": "NAY",
      "confidence": 0.85
    }
  ]
}
```
