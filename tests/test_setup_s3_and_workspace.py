import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from cybergov_evaluate_single_proposal_and_vote import setup_s3_and_workspace


class TestSetupS3AndWorkspace:
    """Integration tests for the setup_s3_and_workspace function."""

    def test_successful_s3_and_workspace_setup(self, temp_workspace):
        """Test successful S3 filesystem initialization and workspace creation."""
        # Mock configuration
        config = {
            "PROPOSAL_ID": "123",
            "NETWORK": "polkadot",
            "S3_BUCKET_NAME": "test-bucket",
            "S3_ACCESS_KEY_ID": "test-key-id",
            "S3_ACCESS_KEY_SECRET": "test-secret",
            "S3_ENDPOINT_URL": "https://test-endpoint.com"
        }
        
        # Change to temp workspace to avoid creating 'workspace' in project root
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            with patch('cybergov_evaluate_single_proposal_and_vote.s3fs.S3FileSystem') as mock_s3fs:
                mock_s3_instance = MagicMock()
                mock_s3fs.return_value = mock_s3_instance
                
                # Call setup_s3_and_workspace
                s3, proposal_s3_path, local_workspace, proposal_id, network = setup_s3_and_workspace(config)
                
                # Verify S3FileSystem was initialized with correct parameters
                mock_s3fs.assert_called_once_with(
                    key="test-key-id",
                    secret="test-secret",
                    client_kwargs={"endpoint_url": "https://test-endpoint.com"},
                    asynchronous=False,
                    loop=None,
                )
                
                # Verify return values
                assert s3 == mock_s3_instance
                assert proposal_s3_path == "test-bucket/proposals/polkadot/123"
                assert local_workspace.name == "workspace"
                assert local_workspace.exists()
                assert proposal_id == "123"
                assert network == "polkadot"
                
        finally:
            os.chdir(original_cwd)

    def test_workspace_creation_idempotent(self, temp_workspace):
        """Test that workspace creation is idempotent (doesn't fail if already exists)."""
        config = {
            "PROPOSAL_ID": "456",
            "NETWORK": "kusama",
            "S3_BUCKET_NAME": "test-bucket",
            "S3_ACCESS_KEY_ID": "test-key-id",
            "S3_ACCESS_KEY_SECRET": "test-secret",
            "S3_ENDPOINT_URL": "https://test-endpoint.com"
        }
        
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            # Create workspace directory first
            workspace_dir = temp_workspace / "workspace"
            workspace_dir.mkdir(exist_ok=True)
            
            with patch('cybergov_evaluate_single_proposal_and_vote.s3fs.S3FileSystem') as mock_s3fs:
                mock_s3_instance = MagicMock()
                mock_s3fs.return_value = mock_s3_instance
                
                # Should not raise an exception
                s3, proposal_s3_path, local_workspace, proposal_id, network = setup_s3_and_workspace(config)
                
                # Verify workspace still exists
                assert local_workspace.exists()
                assert local_workspace.is_dir()
                
        finally:
            os.chdir(original_cwd)

    def test_different_networks_and_proposals(self, temp_workspace):
        """Test that different network and proposal combinations work correctly."""
        test_cases = [
            {"NETWORK": "polkadot", "PROPOSAL_ID": "100"},
            {"NETWORK": "kusama", "PROPOSAL_ID": "200"},
            {"NETWORK": "westend", "PROPOSAL_ID": "300"},
        ]
        
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            for case in test_cases:
                config = {
                    "PROPOSAL_ID": case["PROPOSAL_ID"],
                    "NETWORK": case["NETWORK"],
                    "S3_BUCKET_NAME": "test-bucket",
                    "S3_ACCESS_KEY_ID": "test-key-id",
                    "S3_ACCESS_KEY_SECRET": "test-secret",
                    "S3_ENDPOINT_URL": "https://test-endpoint.com"
                }
                
                with patch('cybergov_evaluate_single_proposal_and_vote.s3fs.S3FileSystem') as mock_s3fs:
                    mock_s3_instance = MagicMock()
                    mock_s3fs.return_value = mock_s3_instance
                    
                    s3, proposal_s3_path, local_workspace, proposal_id, network = setup_s3_and_workspace(config)
                    
                    expected_s3_path = f"test-bucket/proposals/{case['NETWORK']}/{case['PROPOSAL_ID']}"
                    assert proposal_s3_path == expected_s3_path
                    assert proposal_id == case["PROPOSAL_ID"]
                    assert network == case["NETWORK"]
                    
        finally:
            os.chdir(original_cwd)

    def test_s3_configuration_parameters(self, temp_workspace):
        """Test that all S3 configuration parameters are passed correctly."""
        config = {
            "PROPOSAL_ID": "test-proposal",
            "NETWORK": "test-network",
            "S3_BUCKET_NAME": "my-special-bucket",
            "S3_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "S3_ACCESS_KEY_SECRET": "ILOVEPOTATOES",
            "S3_ENDPOINT_URL": "https://s3.amazonaws.com"
        }
        
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            with patch('cybergov_evaluate_single_proposal_and_vote.s3fs.S3FileSystem') as mock_s3fs:
                mock_s3_instance = MagicMock()
                mock_s3fs.return_value = mock_s3_instance
                
                setup_s3_and_workspace(config)
                
                # Verify exact S3FileSystem call
                mock_s3fs.assert_called_once_with(
                    key="AKIAIOSFODNN7EXAMPLE",
                    secret="ILOVEPOTATOES",
                    client_kwargs={"endpoint_url": "https://s3.amazonaws.com"},
                    asynchronous=False,
                    loop=None,
                )
                
        finally:
            os.chdir(original_cwd)

    @patch('cybergov_evaluate_single_proposal_and_vote.logger')
    def test_logging_output(self, mock_logger, temp_workspace):
        """Test that appropriate logging messages are generated."""
        config = {
            "PROPOSAL_ID": "123",
            "NETWORK": "polkadot",
            "S3_BUCKET_NAME": "test-bucket",
            "S3_ACCESS_KEY_ID": "test-key-id",
            "S3_ACCESS_KEY_SECRET": "test-secret",
            "S3_ENDPOINT_URL": "https://test-endpoint.com"
        }
        
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            with patch('cybergov_evaluate_single_proposal_and_vote.s3fs.S3FileSystem') as mock_s3fs:
                mock_s3_instance = MagicMock()
                mock_s3fs.return_value = mock_s3_instance
                
                setup_s3_and_workspace(config)
                
                # Verify logging call
                mock_logger.info.assert_called_with("Working with S3 path: test-bucket/proposals/polkadot/123")
                
        finally:
            os.chdir(original_cwd)

    def test_missing_config_keys(self, temp_workspace):
        """Test that missing configuration keys raise appropriate errors."""
        required_keys = [
            "PROPOSAL_ID", "NETWORK", "S3_BUCKET_NAME", 
            "S3_ACCESS_KEY_ID", "S3_ACCESS_KEY_SECRET", "S3_ENDPOINT_URL"
        ]
        
        base_config = {
            "PROPOSAL_ID": "123",
            "NETWORK": "polkadot",
            "S3_BUCKET_NAME": "test-bucket",
            "S3_ACCESS_KEY_ID": "test-key-id",
            "S3_ACCESS_KEY_SECRET": "test-secret",
            "S3_ENDPOINT_URL": "https://test-endpoint.com"
        }
        
        original_cwd = os.getcwd()
        os.chdir(temp_workspace)
        
        try:
            for missing_key in required_keys:
                config = base_config.copy()
                del config[missing_key]
                
                with patch('cybergov_evaluate_single_proposal_and_vote.s3fs.S3FileSystem'):
                    with pytest.raises(KeyError):
                        setup_s3_and_workspace(config)
                        
        finally:
            os.chdir(original_cwd)
