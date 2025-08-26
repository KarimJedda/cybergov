        logger.info("04 - Submitting MAGI V0 Vote...")
        submit_final_vote.run(s3, proposal_s3_path, network, proposal_id)
        last_good_step = "submitting MAGI V0 Vote"

