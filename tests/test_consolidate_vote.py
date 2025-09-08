import pytest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys
import os

# Add the src directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from cybergov_evaluate_single_proposal_and_vote import consolidate_vote


class TestConsolidateVote:
    """Integration tests for the consolidate_vote function."""

    def test_unanimous_aye_vote(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test consolidation when all agents vote AYE unanimously."""
        # Create analysis files with all AYE votes
        file_data = {
            "balthazar": sample_analysis_data["balthazar_aye"],
            "melchior": sample_analysis_data["melchior_aye"],
            "caspar": sample_analysis_data["caspar_aye"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Verify the vote file was created
        assert vote_path.exists()
        assert vote_path.name == "vote.json"
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        assert vote_data["final_decision"] == "Aye"
        assert vote_data["is_conclusive"] is True
        assert vote_data["is_unanimous"] is True
        assert len(vote_data["votes_breakdown"]) == 3
        
        # Verify all votes are Aye
        for vote in vote_data["votes_breakdown"]:
            assert vote["decision"] == "Aye"
        
        # Verify summary rationale contains expected content
        assert "3 AYE" in vote_data["summary_rationale"]
        assert "0 NAY" in vote_data["summary_rationale"]
        assert "0 ABSTAIN" in vote_data["summary_rationale"]

    def test_majority_nay_vote(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test consolidation when majority votes NAY."""
        # Create analysis files with majority NAY votes
        file_data = {
            "balthazar": sample_analysis_data["balthazar_nay"],
            "melchior": sample_analysis_data["melchior_nay"],
            "caspar": sample_analysis_data["caspar_nay"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        assert vote_data["final_decision"] == "Nay"
        assert vote_data["is_conclusive"] is True
        assert vote_data["is_unanimous"] is True
        assert len(vote_data["votes_breakdown"]) == 3
        
        # Verify summary rationale contains expected content
        assert "0 AYE" in vote_data["summary_rationale"]
        assert "3 NAY" in vote_data["summary_rationale"]

    def test_split_vote_majority_aye(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test consolidation with a split vote where AYE has majority. Should Abstain. """
        # Create analysis files with 2 AYE, 1 NAY
        file_data = {
            "balthazar": sample_analysis_data["balthazar_aye"],
            "melchior": sample_analysis_data["melchior_aye"],
            "caspar": sample_analysis_data["caspar_nay"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        assert vote_data["final_decision"] == "Abstain"
        assert vote_data["is_conclusive"] is False
        assert vote_data["is_unanimous"] is False
        
        # Verify summary rationale contains expected content
        assert "2 AYE" in vote_data["summary_rationale"]
        assert "1 NAY" in vote_data["summary_rationale"]

    def test_tie_vote_results_in_abstain(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test consolidation when there's a tie, should result in ABSTAIN."""
        # Create analysis files with 1 AYE, 1 NAY, 1 ABSTAIN (no clear majority)
        file_data = {
            "balthazar": sample_analysis_data["balthazar_aye"],
            "melchior": sample_analysis_data["melchior_nay"],
            "caspar": sample_analysis_data["caspar_abstain"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        assert vote_data["final_decision"] == "Abstain"
        assert vote_data["is_conclusive"] is False
        assert vote_data["is_unanimous"] is False

    def test_all_abstain_vote(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test consolidation when all agents abstain."""
        # Create analysis files with all ABSTAIN votes
        file_data = {
            "balthazar": sample_analysis_data["balthazar_abstain"],
            "melchior": sample_analysis_data["melchior_abstain"],
            "caspar": sample_analysis_data["caspar_abstain"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        assert vote_data["final_decision"] == "Abstain"
        assert vote_data["is_conclusive"] is True
        assert vote_data["is_unanimous"] is True
        
        # Verify summary rationale contains expected content
        assert "0 AYE" in vote_data["summary_rationale"]
        assert "0 NAY" in vote_data["summary_rationale"]
        assert "3 ABSTAIN" in vote_data["summary_rationale"]

    def test_single_analysis_file(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test consolidation with only one analysis file."""
        # Create single analysis file
        file_data = {
            "balthazar": sample_analysis_data["balthazar_aye"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        assert vote_data["final_decision"] == "Aye"
        assert vote_data["is_conclusive"] is True
        assert vote_data["is_unanimous"] is True
        assert len(vote_data["votes_breakdown"]) == 1

    def test_vote_data_structure(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test that the vote data structure contains all required fields."""
        file_data = {
            "balthazar": sample_analysis_data["balthazar_aye"],
            "melchior": sample_analysis_data["melchior_aye"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data structure
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        # Check required top-level fields
        required_fields = [
            "timestamp_utc", "is_conclusive", "final_decision", 
            "is_unanimous", "summary_rationale", "votes_breakdown"
        ]
        for field in required_fields:
            assert field in vote_data, f"Missing required field: {field}"
        
        # Check votes_breakdown structure
        for vote in vote_data["votes_breakdown"]:
            assert "model" in vote
            assert "decision" in vote
            assert "confidence" in vote
        
        # Check summary_rationale contains HTML structure
        summary = vote_data["summary_rationale"]
        assert "<h1>CYBERGOV V0 - Proposal Analysis</h1>" in summary
        assert "<h2>Vote Summary</h2>" in summary
        assert "<h2>Detailed Rationales</h2>" in summary
        assert f"proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}" in summary

    def test_case_insensitive_decisions(self, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test that decision consolidation works with different case variations."""
        # Create analysis files with mixed case decisions
        file_data = {
            "balthazar": sample_analysis_data["balthazar_aye_lowercase"],
            "melchior": sample_analysis_data["melchior_aye_uppercase"],
            "caspar": sample_analysis_data["caspar_aye"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        # The function should handle case variations properly and normalize to proper case
        assert vote_data["final_decision"] == "Aye"
        assert vote_data["is_conclusive"] is True
        assert vote_data["is_unanimous"] is True
        assert len(vote_data["votes_breakdown"]) == 3
        
        # Verify all decisions are normalized to proper case
        for vote in vote_data["votes_breakdown"]:
            assert vote["decision"] == "Aye"

    def test_empty_analysis_files_list(self, temp_workspace, mock_proposal_data):
        """Test consolidation with empty analysis files list."""
        analysis_files = []
        
        # Call consolidate_vote
        vote_path = consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Load and verify the vote data
        with open(vote_path, 'r') as f:
            vote_data = json.load(f)
        
        assert vote_data["final_decision"] == "Abstain"
        assert vote_data["is_conclusive"] is False
        assert vote_data["is_unanimous"] is False
        assert len(vote_data["votes_breakdown"]) == 0

    @patch('cybergov_evaluate_single_proposal_and_vote.logger')
    def test_logging_output(self, mock_logger, temp_workspace, sample_analysis_data, create_analysis_files, mock_proposal_data):
        """Test that appropriate logging messages are generated."""
        file_data = {
            "balthazar": sample_analysis_data["balthazar_aye"]
        }
        
        analysis_files = create_analysis_files(temp_workspace, file_data)
        
        # Call consolidate_vote
        consolidate_vote(
            analysis_files, 
            temp_workspace, 
            mock_proposal_data["proposal_id"], 
            mock_proposal_data["network"]
        )
        
        # Verify logging calls
        mock_logger.info.assert_any_call("03 - Consolidating vote...")
        mock_logger.info.assert_any_call(f"✅ Vote consolidated into {temp_workspace / 'vote.json'}.")
