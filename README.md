# cybergov

An experiment to understand how LLMs can be useful for governance decisions. Inspired from Evangelion. 

This is a work in progress, i'm still noodling around on it. 

## Links and information

- [What is this?](https://forum.polkadot.network/t/decentralized-voices-cohort-5-light-track-karim-cybergov/14254)
- [How does this work?](https://forum.polkadot.network/t/cybergov-v0-automating-trust-verifiable-llm-governance-on-polkadot/14796)
- [What's next?](https://github.com/KarimJedda/cybergov/discussions/2)
- [Where can I see the votes?](https://polkadot.subsquare.io/referenda/dv) (click on "Guardian")


## Quick links

- I'd like to request a re-vote on my proposal or another proposal: [Open an issue](https://github.com/KarimJedda/cybergov/issues)
- I'd like to follow the developpements: [Follow the announcements](https://github.com/KarimJedda/cybergov/discussions/categories/announcements) or [Follow me](https://x.com/KarimJDDA)
- I have a suggestion or an idea: [Join the discussion](https://github.com/KarimJedda/cybergov/discussions)


## Accounts used 

```
## Identity cannot be set with Vault on Paseo, cf https://github.com/novasamatech/metadata-portal/issues/1367 
CYBERGOV_PASEO_MAIN_PUBKEY      = "13Q56KnUmLNe8fomKD3hoY38ZwLKZgRGdY4RTovRNFjMSwKw"
CYBERGOV_PASEO_PROXY_PUBKEY     = "14zNhvyLnJKtYRmfptavEPWHuV9qEXZQNqXCjDmnvjrg1gtL"

CYBERGOV_POLKADOT_MAIN_PUBKEY   = "13Q56KnUmLNe8fomKD3hoY38ZwLKZgRGdY4RTovRNFjMSwKw"
CYBERGOV_POLKADOT_PROXY_PUBKEY  = "15DbGtWxaAU6tDPpdZhP9QyVZZWdSXaGCjD88cRZhhdCKTjE"

CYBERGOV_KUSAMA_MAIN_PUBKEY     = "EyPcJsHXv86Snch8GokZLZyrucug3gK1RAghBD2HxvL1YRZ"
CYBERGOV_KUSAMA_PROXY_PUBKEY    = "GWUyiyVmA6pbubhM9h7A6qGDqTJKJK3L3YoJsWe6DP7m67a"
```

## Operator manual 


Field guide for Ikari apprentices.

```
cd src/

# step 1
python cybergov_data_scraper.py <network> <proposal_id>

# step 2.alpha (run inference locally for debugging)
PROPOSAL_ID=<proposal_id> NETWORK=<network>  S3_ENDPOINT_URL=XXX  S3_BUCKET_NAME=XX S3_ACCESS_KEY_ID=XXX S3_ACCESS_KEY_SECRET=XXX OPENROUTER_API_KEY="XXX" python cybergov_evaluate_single_proposal_and_vote.py

# step 2.beta (run inference remotely on GitHub, only way to vote)
python cybergov_inference.py <network> <proposal_id>

# step 3 
python cybergov_voter.py <network> <proposal_id>

# step 4
python cybergov_commenter.py <network> <proposal_id>

```
