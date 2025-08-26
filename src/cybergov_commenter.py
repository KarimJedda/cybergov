        logger.info("05 - Posting Subsquare comment...")
        post_subsquare_comment.run(s3, proposal_s3_path, network, proposal_id)
        last_good_step = "posting Subsquare comment"
        
        logger.info("🎉 Proposal processing and voting finished successfully!")