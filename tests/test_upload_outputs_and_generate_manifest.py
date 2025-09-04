import pytest
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

# Add the src directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from cybergov_evaluate_single_proposal_and_vote import upload_outputs_and_generate_manifest


class TestUploadOutputsAndGenerateManifest:
    """Integration tests for the upload_outputs_and_generate_manifest function."""

    def test_successful_upload_and_manifest_generation(self, temp_workspace):
        """Test successful file upload and manifest generation."""
        # Create mock analysis files
        analysis_dir = temp_workspace / "llm_analyses"
        analysis_dir.mkdir(exist_ok=True)
        
        analysis_files = []
        for magi in ["balthazar", "melchior", "caspar"]:
            analysis_file = analysis_dir / f"{magi}.json"
            with open(analysis_file, 'w') as f:
                json.dump({
                    "model_name": f"test-model-{magi}",
                    "decision": "Aye",
                    "rationale": f"Test rationale from {magi}"
                }, f)
            analysis_files.append(analysis_file)
        
        # Create mock vote file
        vote_file = temp_workspace / "vote.json"
        with open(vote_file, 'w') as f:
            json.dump({
                "final_decision": "Aye",
                "is_conclusive": True,
                "votes_breakdown": []
            }, f)
        
        # Mock S3 filesystem
        mock_s3 = MagicMock()
        
        # Mock manifest inputs
        manifest_inputs = [
            {
                "logical_name": "raw_subsquare_data",
                "s3_path": "test-bucket/proposals/polkadot/123/raw_subsquare_data.json",
                "hash": "test-hash-1"
            },
            {
                "logical_name": "content_markdown",
                "s3_path": "test-bucket/proposals/polkadot/123/content.md",
                "hash": "test-hash-2"
            }
        ]
        
        proposal_s3_path = "test-bucket/proposals/polkadot/123"
        
        with patch('cybergov_evaluate_single_proposal_and_vote.hash_file') as mock_hash_file:
            mock_hash_file.side_effect = lambda x: f"hash-{x.stem}"
            
            with patch.dict(os.environ, {
                'GITHUB_REPOSITORY': 'test/repo',
                'GITHUB_RUN_ID': '12345',
                'GITHUB_SHA': 'abcdef123456'
            }):
                # Call the function
                manifest = upload_outputs_and_generate_manifest(
                    mock_s3, proposal_s3_path, temp_workspace, 
                    analysis_files, vote_file, manifest_inputs
                )
        
        # Verify S3 uploads were called
        expected_uploads = [
            ("llm_analyses/balthazar.json", f"{proposal_s3_path}/llm_analyses/balthazar.json"),
            ("llm_analyses/melchior.json", f"{proposal_s3_path}/llm_analyses/melchior.json"),
            ("llm_analyses/caspar.json", f"{proposal_s3_path}/llm_analyses/caspar.json"),
            ("vote.json", f"{proposal_s3_path}/vote.json"),
            ("manifest.json", f"{proposal_s3_path}/manifest.json")
        ]
        
        assert mock_s3.upload.call_count == 5
        
        # Verify manifest structure
        assert "provenance" in manifest
        assert "inputs" in manifest
        assert "outputs" in manifest
        
        # Verify provenance
        provenance = manifest["provenance"]
        assert provenance["job_name"] == "LLM Inference and Voting"
        assert provenance["github_repository"] == "test/repo"
        assert provenance["github_run_id"] == "12345"
        assert provenance["github_commit_sha"] == "abcdef123456"
        assert "timestamp_utc" in provenance
        
        # Verify inputs are preserved
        assert manifest["inputs"] == manifest_inputs
        
        # Verify outputs
        outputs = manifest["outputs"]
        assert len(outputs) == 4  # 3 analysis files + 1 vote file
        
        # Check analysis file outputs
        analysis_outputs = [o for o in outputs if o["logical_name"] in ["balthazar", "melchior", "caspar"]]
        assert len(analysis_outputs) == 3
        
        for output in analysis_outputs:
            assert output["s3_path"].startswith(f"{proposal_s3_path}/llm_analyses/")
            assert output["hash"].startswith("hash-")
        
        # Check vote file output
        vote_output = next(o for o in outputs if o["logical_name"] == "vote")
        assert vote_output["s3_path"] == f"{proposal_s3_path}/vote.json"
        assert vote_output["hash"] == "hash-vote"
        
        # Verify manifest.json was created locally
        manifest_path = temp_workspace / "manifest.json"
        assert manifest_path.exists()
        
        with open(manifest_path, 'r') as f:
            saved_manifest = json.load(f)
        assert saved_manifest == manifest

    def test_empty_analysis_files(self, temp_workspace):
        """Test handling of empty analysis files list."""
        # Create only vote file
        vote_file = temp_workspace / "vote.json"
        with open(vote_file, 'w') as f:
            json.dump({"final_decision": "Abstain"}, f)
        
        mock_s3 = MagicMock()
        manifest_inputs = []
        proposal_s3_path = "test-bucket/proposals/polkadot/123"
        
        with patch('cybergov_evaluate_single_proposal_and_vote.hash_file') as mock_hash_file:
            mock_hash_file.return_value = "hash-vote"
            
            manifest = upload_outputs_and_generate_manifest(
                mock_s3, proposal_s3_path, temp_workspace, 
                [], vote_file, manifest_inputs
            )
        
        # Should upload vote file and manifest only
        assert mock_s3.upload.call_count == 2
        
        # Should have only one output (vote file)
        assert len(manifest["outputs"]) == 1
        assert manifest["outputs"][0]["logical_name"] == "vote"

    def test_mixed_file_types_s3_path_generation(self, temp_workspace):
        """Test that S3 paths are generated correctly for different file types."""
        # Create files in different locations
        analysis_dir = temp_workspace / "llm_analyses"
        analysis_dir.mkdir(exist_ok=True)
        
        analysis_file = analysis_dir / "balthazar.json"
        with open(analysis_file, 'w') as f:
            json.dump({"test": "data"}, f)
        
        vote_file = temp_workspace / "vote.json"
        with open(vote_file, 'w') as f:
            json.dump({"final_decision": "Aye"}, f)
        
        other_file = temp_workspace / "other.json"
        with open(other_file, 'w') as f:
            json.dump({"other": "data"}, f)
        
        mock_s3 = MagicMock()
        proposal_s3_path = "test-bucket/proposals/polkadot/123"
        
        with patch('cybergov_evaluate_single_proposal_and_vote.hash_file') as mock_hash_file:
            mock_hash_file.side_effect = lambda x: f"hash-{x.stem}"
            
            manifest = upload_outputs_and_generate_manifest(
                mock_s3, proposal_s3_path, temp_workspace, 
                [analysis_file], other_file, []  # Using other_file as vote_file
            )
        
        # Verify S3 paths
        outputs = manifest["outputs"]
        
        # Analysis file should have llm_analyses/ prefix
        analysis_output = next(o for o in outputs if o["logical_name"] == "balthazar")
        assert analysis_output["s3_path"] == f"{proposal_s3_path}/llm_analyses/balthazar.json"
        
        # Other file should not have prefix
        other_output = next(o for o in outputs if o["logical_name"] == "other")
        assert other_output["s3_path"] == f"{proposal_s3_path}/other.json"

    def test_environment_variables_handling(self, temp_workspace):
        """Test handling of missing environment variables."""
        vote_file = temp_workspace / "vote.json"
        with open(vote_file, 'w') as f:
            json.dump({"final_decision": "Aye"}, f)
        
        mock_s3 = MagicMock()
        
        with patch('cybergov_evaluate_single_proposal_and_vote.hash_file') as mock_hash_file:
            mock_hash_file.return_value = "test-hash"
            
            # Test with no environment variables
            with patch.dict(os.environ, {}, clear=True):
                manifest = upload_outputs_and_generate_manifest(
                    mock_s3, "test-path", temp_workspace, [], vote_file, []
                )
            
            provenance = manifest["provenance"]
            assert provenance["github_repository"] == "N/A"
            assert provenance["github_run_id"] == "N/A"
            assert provenance["github_commit_sha"] == "N/A"

    @patch('cybergov_evaluate_single_proposal_and_vote.logger')
    def test_logging_output(self, mock_logger, temp_workspace):
        """Test that appropriate logging messages are generated."""
        # Create minimal test files
        vote_file = temp_workspace / "vote.json"
        with open(vote_file, 'w') as f:
            json.dump({"final_decision": "Aye"}, f)
        
        mock_s3 = MagicMock()
        
        with patch('cybergov_evaluate_single_proposal_and_vote.hash_file') as mock_hash_file:
            mock_hash_file.return_value = "test-hash"
            
            upload_outputs_and_generate_manifest(
                mock_s3, "test-path", temp_workspace, [], vote_file, []
            )
        
        # Verify logging calls
        mock_logger.info.assert_any_call("04 - Attesting, signing, and uploading outputs...")
        mock_logger.info.assert_any_call("  📤 Uploaded vote.json to test-path/vote.json")
        mock_logger.info.assert_any_call("✅ Uploaded manifest.")

    def test_file_hash_integration(self, temp_workspace):
        """Test that file hashing is properly integrated."""
        vote_file = temp_workspace / "vote.json"
        vote_content = {"final_decision": "Aye", "test": "data"}
        with open(vote_file, 'w') as f:
            json.dump(vote_content, f)
        
        mock_s3 = MagicMock()
        
        with patch('cybergov_evaluate_single_proposal_and_vote.hash_file') as mock_hash_file:
            mock_hash_file.return_value = "actual-file-hash"
            
            manifest = upload_outputs_and_generate_manifest(
                mock_s3, "test-path", temp_workspace, [], vote_file, []
            )
        
        # Verify hash_file was called with the correct file
        mock_hash_file.assert_called_with(vote_file)
        
        # Verify hash is in manifest
        assert manifest["outputs"][0]["hash"] == "actual-file-hash"

    def test_manifest_timestamp_format(self, temp_workspace):
        """Test that manifest timestamp is in correct ISO format."""
        vote_file = temp_workspace / "vote.json"
        with open(vote_file, 'w') as f:
            json.dump({"final_decision": "Aye"}, f)
        
        mock_s3 = MagicMock()
        
        with patch('cybergov_evaluate_single_proposal_and_vote.hash_file') as mock_hash_file:
            mock_hash_file.return_value = "test-hash"
            
            manifest = upload_outputs_and_generate_manifest(
                mock_s3, "test-path", temp_workspace, [], vote_file, []
            )
        
        timestamp = manifest["provenance"]["timestamp_utc"]
        
        # Should be able to parse as ISO format
        parsed_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        assert parsed_timestamp.tzinfo is not None
