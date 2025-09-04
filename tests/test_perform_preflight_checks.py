import pytest
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from cybergov_evaluate_single_proposal_and_vote import perform_preflight_checks


class TestPerformPreflightChecks:
    """Integration tests for the perform_preflight_checks function."""

    def test_successful_preflight_checks(self, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data):
        """Test successful preflight checks with all required files present."""
        # Change to temp workspace to make system prompts discoverable
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Call perform_preflight_checks
            manifest_inputs, local_content_path, magi_models = perform_preflight_checks(
                mock_s3_filesystem, proposal_s3_path, temp_workspace
            )
            
            # Verify return values
            assert len(manifest_inputs) == 2  # raw_subsquare_data and content_markdown
            assert local_content_path.name == "content.md"
            assert local_content_path.exists()
            assert magi_models == ["balthazar", "caspar", "melchior"]
            
            # Verify manifest inputs structure
            raw_data_input = next(item for item in manifest_inputs if item["logical_name"] == "raw_subsquare_data")
            content_input = next(item for item in manifest_inputs if item["logical_name"] == "content_markdown")
            
            assert "s3_path" in raw_data_input
            assert "hash" in raw_data_input
            assert "s3_path" in content_input
            assert "hash" in content_input
            
            # Verify local files were created
            raw_local_path = temp_workspace / "raw_subsquare.json"
            assert raw_local_path.exists()
            
            # Verify downloaded content
            with open(raw_local_path, 'r') as f:
                raw_data = json.load(f)
            assert raw_data["referendumIndex"] == 123
            assert raw_data["title"] == "Test Proposal"
            
            with open(local_content_path, 'r') as f:
                content = f.read()
            assert "Test Proposal" in content
            
        finally:
            os.chdir(original_cwd)

    def test_missing_raw_subsquare_file(self, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data):
        """Test failure when raw_subsquare_data.json is missing from S3."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            # Configure mock to return False for raw_subsquare_data.json
            def mock_exists(path):
                return 'raw_subsquare_data.json' not in path
            
            mock_s3_filesystem.exists.side_effect = mock_exists
            
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Should raise FileNotFoundError
            with pytest.raises(FileNotFoundError) as exc_info:
                perform_preflight_checks(mock_s3_filesystem, proposal_s3_path, temp_workspace)
            
            assert "raw_subsquare_data.json" in str(exc_info.value)
            
        finally:
            os.chdir(original_cwd)

    def test_missing_content_md_file(self, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data):
        """Test failure when content.md is missing from S3."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            # Configure mock to return False for content.md
            def mock_exists(path):
                return 'content.md' not in path
            
            mock_s3_filesystem.exists.side_effect = mock_exists
            
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Should raise FileNotFoundError
            with pytest.raises(FileNotFoundError) as exc_info:
                perform_preflight_checks(mock_s3_filesystem, proposal_s3_path, temp_workspace)
            
            assert "content.md" in str(exc_info.value)
            
        finally:
            os.chdir(original_cwd)

    def test_invalid_raw_subsquare_data(self, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data, sample_raw_subsquare_data):
        """Test failure when raw_subsquare_data.json has missing required attributes."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            # Configure mock to return invalid data
            def mock_open_context(path, mode='r'):
                mock_file = MagicMock()
                if 'raw_subsquare_data.json' in path:
                    mock_file.__enter__.return_value.read.return_value = json.dumps(
                        sample_raw_subsquare_data["missing_title"]
                    )
                elif 'content.md' in path:
                    mock_file.__enter__.return_value.read.return_value = "# Test Content"
                mock_file.__exit__ = MagicMock(return_value=None)
                return mock_file
            
            mock_s3_filesystem.open.side_effect = mock_open_context
            
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Should raise ValueError
            with pytest.raises(ValueError) as exc_info:
                perform_preflight_checks(mock_s3_filesystem, proposal_s3_path, temp_workspace)
            
            assert "missing one of the required attributes" in str(exc_info.value)
            assert "title" in str(exc_info.value)
            
        finally:
            os.chdir(original_cwd)

    def test_missing_system_prompt_file(self, temp_workspace, mock_s3_filesystem, mock_proposal_data):
        """Test failure when a system prompt file is missing."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            # Create incomplete system prompts (missing melchior)
            prompts_dir = temp_workspace / "templates" / "system_prompts"
            prompts_dir.mkdir(parents=True, exist_ok=True)
            
            for model in ["balthazar", "caspar"]:  # Missing melchior
                prompt_file = prompts_dir / f"{model}_system_prompt.md"
                with open(prompt_file, 'w') as f:
                    f.write(f"# {model.title()} System Prompt")
            
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Should raise FileNotFoundError
            with pytest.raises(FileNotFoundError) as exc_info:
                perform_preflight_checks(mock_s3_filesystem, proposal_s3_path, temp_workspace)
            
            assert "melchior_system_prompt.md" in str(exc_info.value)
            
        finally:
            os.chdir(original_cwd)

    def test_empty_raw_subsquare_data(self, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data, sample_raw_subsquare_data):
        """Test failure when raw_subsquare_data.json is empty."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            # Configure mock to return empty data
            def mock_open_context(path, mode='r'):
                mock_file = MagicMock()
                if 'raw_subsquare_data.json' in path:
                    mock_file.__enter__.return_value.read.return_value = json.dumps(
                        sample_raw_subsquare_data["empty_data"]
                    )
                elif 'content.md' in path:
                    mock_file.__enter__.return_value.read.return_value = "# Test Content"
                mock_file.__exit__ = MagicMock(return_value=None)
                return mock_file
            
            mock_s3_filesystem.open.side_effect = mock_open_context
            
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Should raise ValueError
            with pytest.raises(ValueError) as exc_info:
                perform_preflight_checks(mock_s3_filesystem, proposal_s3_path, temp_workspace)
            
            assert "missing one of the required attributes" in str(exc_info.value)
            
        finally:
            os.chdir(original_cwd)

    def test_valid_data_with_extra_fields(self, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data, sample_raw_subsquare_data):
        """Test that extra fields in raw_subsquare_data.json are allowed."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            # Configure mock to return data with extra fields
            def mock_open_context(path, mode='r'):
                mock_file = MagicMock()
                if 'raw_subsquare_data.json' in path:
                    mock_file.__enter__.return_value.read.return_value = json.dumps(
                        sample_raw_subsquare_data["valid_data"]
                    )
                elif 'content.md' in path:
                    mock_file.__enter__.return_value.read.return_value = "# Test Content"
                mock_file.__exit__ = MagicMock(return_value=None)
                return mock_file
            
            mock_s3_filesystem.open.side_effect = mock_open_context
            
            # Update download mock to handle the valid data
            def mock_download(s3_path, local_path):
                local_file = Path(local_path)
                local_file.parent.mkdir(parents=True, exist_ok=True)
                if 'raw_subsquare_data.json' in s3_path:
                    with open(local_file, 'w') as f:
                        json.dump(sample_raw_subsquare_data["valid_data"], f)
                elif 'content.md' in s3_path:
                    with open(local_file, 'w') as f:
                        f.write("# Test Content")
            
            mock_s3_filesystem.open.side_effect = mock_open_context
            mock_s3_filesystem.download.side_effect = mock_download
            
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Should succeed
            manifest_inputs, local_content_path, magi_models = perform_preflight_checks(
                mock_s3_filesystem, proposal_s3_path, temp_workspace
            )
            
            # Verify it worked
            assert len(manifest_inputs) == 2
            assert local_content_path.exists()
            assert magi_models == ["balthazar", "caspar", "melchior"]
            
        finally:
            os.chdir(original_cwd)

    @patch('cybergov_evaluate_single_proposal_and_vote.logger')
    def test_logging_output(self, mock_logger, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data):
        """Test that appropriate logging messages are generated."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Call perform_preflight_checks
            perform_preflight_checks(mock_s3_filesystem, proposal_s3_path, temp_workspace)
            
            # Verify logging calls
            mock_logger.info.assert_any_call("01 - Performing pre-flight data checks...")
            mock_logger.info.assert_any_call("✅ raw_subsquare.json found and validated.")
            mock_logger.info.assert_any_call("✅ content.md found.")
            mock_logger.info.assert_any_call("✅ All local system prompts found.")
            mock_logger.info.assert_any_call("Pre-flight checks passed.")
            
        finally:
            os.chdir(original_cwd)

    def test_s3_path_construction(self, temp_workspace, mock_s3_filesystem, mock_system_prompts, mock_proposal_data):
        """Test that S3 paths are constructed correctly."""
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            proposal_s3_path = f"test-bucket/proposals/{mock_proposal_data['network']}/{mock_proposal_data['proposal_id']}"
            
            # Call perform_preflight_checks
            perform_preflight_checks(mock_s3_filesystem, proposal_s3_path, temp_workspace)
            
            # Verify S3 exists calls were made with correct paths
            expected_raw_path = f"{proposal_s3_path}/raw_subsquare_data.json"
            expected_content_path = f"{proposal_s3_path}/content.md"
            
            mock_s3_filesystem.exists.assert_any_call(expected_raw_path)
            mock_s3_filesystem.exists.assert_any_call(expected_content_path)
            
        finally:
            os.chdir(original_cwd)
