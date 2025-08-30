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
├── raw_subsquare_data.json		  # Raw data extracted from Subsquare
├── content.md 				      # Cleaned, extracted content for the LLMs
├── llm_analyses/           # NEW: Directory for individual LLM outputs
│   ├── balthazar.json
│   ├── caspar.json
│   └── melchior.json
├── manifest.json           # hashes of inputs / outputs for provenance
└── vote.json 		          # The final vote result
```

network will be one of polkadot, kusama, paseo. These files will be publicly accessible but served through a CDN. 

The vote will be bundled in a utility batch call, with the SHA256(manifest.json) as a justification! 


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


## Account structure

Governance proxies, as sub-accounts. Cannot be pure because we need to sign a message to post a comment on Subsquare. 

cybergov-main <- identity etc will be set here
├── cybergov/ikari (Polkadot Mainnet) <- will be sub identity with Governance Proxy for the main account
├── cybergov/akagi (Kusama Mainnet)   
└── cybergov/akira (Polkadot Testnet aka Paseo) 


When voting, one of the proxy posts the vote, along with the SHA256 of the manifest.json as a system remark. People then can indepently verify / scrutinize each vote decision. 

From any chain action of these accounts, it's possible to link it to a signed Subsquare comment. 
From any signed Subsquare comment, it's possible to follow the on-chain info, by correlating SHA256(manifest.json) with the system.remark input. 

Question: What of re-votes? Sometimes we have to run the voting again. Perhaps we should bake in the fact that there will be several votes, or create folders for each vote. Then it's possible to diff. 

### Chain specifics 

```
## Identity cannot be set with Vault on Paseo, cf https://github.com/novasamatech/metadata-portal/issues/1367 
CYBERGOV_PASEO_MAIN_PUBKEY      = "13Q56KnUmLNe8fomKD3hoY38ZwLKZgRGdY4RTovRNFjMSwKw"
CYBERGOV_PASEO_PROXY_PUBKEY     = "14zNhvyLnJKtYRmfptavEPWHuV9qEXZQNqXCjDmnvjrg1gtL"

CYBERGOV_POLKADOT_MAIN_PUBKEY   = "13Q56KnUmLNe8fomKD3hoY38ZwLKZgRGdY4RTovRNFjMSwKw"
CYBERGOV_POLKADOT_PROXY_PUBKEY  = "15DbGtWxaAU6tDPpdZhP9QyVZZWdSXaGCjD88cRZhhdCKTjE"

CYBERGOV_KUSAMA_MAIN_PUBKEY     = "EyPcJsHXv86Snch8GokZLZyrucug3gK1RAghBD2HxvL1YRZ"
CYBERGOV_KUSAMA_PROXY_PUBKEY    = "GWUyiyVmA6pbubhM9h7A6qGDqTJKJK3L3YoJsWe6DP7m67a"
```

## Notes on migration

Very smooth sailing:

- Sidecar: replace with the ones pointing to the AssetHubs
- Subsquare: need to test and make sure, maybe pause Cybergov for 1 or 2 days after migration once manual checks on Subsquare pass