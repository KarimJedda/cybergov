import pytest
import json
import hashlib
from unittest.mock import Mock, patch, MagicMock
from substrateinterface import Keypair
import s3fs
import logging

import sys
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Mock Prefect components to prevent API calls during tests
@pytest.fixture(autouse=True)
def mock_prefect_components():
    """Mock Prefect components to prevent API calls during tests"""
    with patch('cybergov_voter.get_run_logger') as mock_logger, \
         patch('cybergov_voter.task') as mock_task:
        # Return a standard Python logger instead of Prefect logger
        mock_logger.return_value = logging.getLogger('test_logger')
        # Make @task decorator a no-op (just return the function unchanged)
        mock_task.side_effect = lambda func: func
        yield mock_logger, mock_task

from cybergov_voter import (
    get_inference_result,
    create_and_sign_vote_tx,
    should_we_vote,
    setup_s3_filesystem,
    create_vote_parameters,
    get_remark_hash,
    VoteResult,
)


class TestVoterData:
    """Test data container for cybergov_voter tests"""
    
    @staticmethod
    def get_valid_vote_data():
        """Returns valid vote.json data"""
        return {
            "timestamp_utc": "2024-01-01T12:00:00Z",
            "is_conclusive": True,
            "final_decision": "AYE",
            "is_unanimous": False,
            "summary_rationale": "Test rationale",
            "votes_breakdown": [
                {"model": "balthazar", "decision": "Aye", "confidence": None},
                {"model": "melchior", "decision": "Aye", "confidence": None},
                {"model": "caspar", "decision": "Nay", "confidence": None}
            ]
        }
    
    @staticmethod
    def get_valid_manifest_data():
        """Returns valid manifest.json data"""
        return {
            "provenance": {
                "job_name": "LLM Inference and Voting",
                "github_repository": "test/repo",
                "github_run_id": "123",
                "github_commit_sha": "abc123",
                "timestamp_utc": "2024-01-01T12:00:00Z"
            },
            "inputs": [],
            "outputs": []
        }
    
    @staticmethod
    def get_valid_subsquare_data():
        """Returns valid raw_subsquare_data.json"""
        return {
            "referendumIndex": 100,
            "title": "Test Proposal",
            "content": "Test content",
            "proposer": "test_proposer",
            "track": 34  # Valid track ID
        }
    
    @staticmethod
    def get_invalid_track_subsquare_data():
        """Returns subsquare data with invalid track"""
        data = TestVoterData.get_valid_subsquare_data()
        data["track"] = 999  # Invalid track ID
        return data
    
    @staticmethod
    def generate_test_keypair():
        """Generate a fresh test keypair"""
        return Keypair.create_from_mnemonic(Keypair.generate_mnemonic())


class TestSetupS3Filesystem:
    """Test S3 filesystem setup"""
    
    def test_setup_s3_filesystem(self):
        """Test S3 filesystem initialization"""
        access_key = "test_access_key"
        secret_key = "test_secret_key"
        endpoint_url = "https://test.endpoint.com"
        
        with patch('src.cybergov_voter.s3fs.S3FileSystem') as mock_s3fs:
            mock_instance = Mock()
            mock_s3fs.return_value = mock_instance
            
            result = setup_s3_filesystem(access_key, secret_key, endpoint_url)
            
            mock_s3fs.assert_called_once_with(
                key=access_key,
                secret=secret_key,
                client_kwargs={"endpoint_url": endpoint_url},
                asynchronous=False,
                loop=None,
            )
            assert result == mock_instance


class TestCreateVoteParameters:
    """Test vote parameter creation"""
    
    def test_create_vote_parameters_aye(self):
        """Test creating Aye vote parameters"""
        result = create_vote_parameters("Aye", "polkadot")
        
        expected = {
            "Standard": {
                "vote": {
                    "aye": True,
                    "conviction": 'Locked1x',  # CONVICTION_MAPPING[1]
                },
                "balance": 10000000000,  # voting_power["polkadot"]
            }
        }
        assert result == expected
    
    def test_create_vote_parameters_nay(self):
        """Test creating Nay vote parameters"""
        result = create_vote_parameters("Nay", "kusama")
        
        expected = {
            "Standard": {
                "vote": {
                    "aye": False,
                    "conviction": 'Locked2x',  # CONVICTION_MAPPING[2]
                },
                "balance": 1000000000000,  # voting_power["kusama"]
            }
        }
        assert result == expected
    
    def test_create_vote_parameters_abstain(self):
        """Test creating Abstain vote parameters"""
        result = create_vote_parameters("Abstain", "polkadot")
        
        expected = {
            "SplitAbstain": {
                "aye": 0,
                "nay": 0,
                "abstain": 10000000000,  # voting_power["polkadot"]
            }
        }
        assert result == expected


class TestGetRemarkHash:
    """Test remark hash generation"""
    
    def test_get_remark_hash(self):
        """Test generating canonical hash from manifest"""
        test_data = TestVoterData.get_valid_manifest_data()
        
        # Create mock S3 filesystem
        mock_s3 = Mock()
        mock_file = Mock()
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=None)
        mock_s3.open.return_value = mock_file
        
        with patch('json.load', return_value=test_data):
            result = get_remark_hash(mock_s3, "test/path/manifest.json")
            
            # Verify the hash is consistent
            canonical_manifest = json.dumps(
                test_data, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            expected_hash = hashlib.sha256(canonical_manifest).hexdigest()
            
            assert result == expected_hash
            mock_s3.open.assert_called_once_with("test/path/manifest.json", "rb")


class TestGetInferenceResult:
    """Test inference result retrieval"""
    
    @patch('cybergov_voter.setup_s3_filesystem')
    def test_get_inference_result_success(self, mock_setup_s3):
        """Test successful inference result retrieval"""
        # Setup test data
        vote_data = TestVoterData.get_valid_vote_data()
        manifest_data = TestVoterData.get_valid_manifest_data()
        
        # Mock S3 filesystem
        mock_s3 = Mock()
        mock_setup_s3.return_value = mock_s3
        
        # Mock file operations
        vote_file_mock = Mock()
        vote_file_mock.__enter__ = Mock(return_value=vote_file_mock)
        vote_file_mock.__exit__ = Mock(return_value=None)
        
        manifest_file_mock = Mock()
        manifest_file_mock.__enter__ = Mock(return_value=manifest_file_mock)
        manifest_file_mock.__exit__ = Mock(return_value=None)
        
        def mock_open(path, mode):
            if "vote.json" in path:
                return vote_file_mock
            elif "manifest.json" in path:
                return manifest_file_mock
            
        mock_s3.open.side_effect = mock_open
        
        with patch('json.load') as mock_json_load:
            mock_json_load.side_effect = [vote_data, manifest_data]
            
            with patch('cybergov_voter.get_remark_hash', return_value="test_hash"):
                # Use .fn() to call the underlying function directly
                result = get_inference_result.fn(
                    network="polkadot",
                    proposal_id=100,
                    s3_bucket="test-bucket",
                    endpoint_url="https://test.endpoint.com",
                    access_key="test_key",
                    secret_key="test_secret"
                )
                
                vote_result, conviction, remark_text, returned_vote_data = result
                
                assert vote_result == VoteResult.AYE
                assert conviction == 1  # CONVICTION_DEFAULT
                assert remark_text == "test_hash"
                assert returned_vote_data == vote_data
    
    @patch('cybergov_voter.setup_s3_filesystem')
    def test_get_inference_result_invalid_vote(self, mock_setup_s3):
        """Test handling of invalid vote decision"""
        vote_data = TestVoterData.get_valid_vote_data()
        vote_data["final_decision"] = "INVALID"
        
        mock_s3 = Mock()
        mock_setup_s3.return_value = mock_s3
        
        vote_file_mock = Mock()
        vote_file_mock.__enter__ = Mock(return_value=vote_file_mock)
        vote_file_mock.__exit__ = Mock(return_value=None)
        mock_s3.open.return_value = vote_file_mock
        
        with patch('json.load', return_value=vote_data):
            with pytest.raises(ValueError, match="Invalid 'final_decision' in vote.json"):
                get_inference_result.fn(
                    network="polkadot",
                    proposal_id=100,
                    s3_bucket="test-bucket",
                    endpoint_url="https://test.endpoint.com",
                    access_key="test_key",
                    secret_key="test_secret"
                )
    
    @patch('cybergov_voter.setup_s3_filesystem')
    def test_get_inference_result_file_not_found(self, mock_setup_s3):
        """Test handling of missing vote file"""
        mock_s3 = Mock()
        mock_setup_s3.return_value = mock_s3
        mock_s3.open.side_effect = FileNotFoundError("File not found")
        
        result = get_inference_result.fn(
            network="polkadot",
            proposal_id=100,
            s3_bucket="test-bucket",
            endpoint_url="https://test.endpoint.com",
            access_key="test_key",
            secret_key="test_secret"
        )
        
        assert result == (None, None, None)


class TestShouldWeVote:
    """Test vote decision logic"""
    
    @patch('cybergov_voter.setup_s3_filesystem')
    def test_should_we_vote_valid_track(self, mock_setup_s3):
        """Test voting decision with valid track"""
        subsquare_data = TestVoterData.get_valid_subsquare_data()
        
        mock_s3 = Mock()
        mock_setup_s3.return_value = mock_s3
        
        file_mock = Mock()
        file_mock.__enter__ = Mock(return_value=file_mock)
        file_mock.__exit__ = Mock(return_value=None)
        mock_s3.open.return_value = file_mock
        
        with patch('json.load', return_value=subsquare_data):
            result = should_we_vote.fn(
                network="polkadot",
                proposal_id=100,
                s3_bucket="test-bucket",
                endpoint_url="https://test.endpoint.com",
                access_key="test_key",
                secret_key="test_secret"
            )
            
            assert result is True
    
    @patch('cybergov_voter.setup_s3_filesystem')
    def test_should_we_vote_invalid_track(self, mock_setup_s3):
        """Test voting decision with invalid track"""
        subsquare_data = TestVoterData.get_invalid_track_subsquare_data()
        
        mock_s3 = Mock()
        mock_setup_s3.return_value = mock_s3
        
        file_mock = Mock()
        file_mock.__enter__ = Mock(return_value=file_mock)
        file_mock.__exit__ = Mock(return_value=None)
        mock_s3.open.return_value = file_mock
        
        with patch('json.load', return_value=subsquare_data):
            result = should_we_vote.fn(
                network="polkadot",
                proposal_id=100,
                s3_bucket="test-bucket",
                endpoint_url="https://test.endpoint.com",
                access_key="test_key",
                secret_key="test_secret"
            )
            
            assert result is False
    
    @patch('cybergov_voter.setup_s3_filesystem')
    def test_should_we_vote_no_track(self, mock_setup_s3):
        """Test voting decision with missing track"""
        subsquare_data = TestVoterData.get_valid_subsquare_data()
        del subsquare_data["track"]
        
        mock_s3 = Mock()
        mock_setup_s3.return_value = mock_s3
        
        file_mock = Mock()
        file_mock.__enter__ = Mock(return_value=file_mock)
        file_mock.__exit__ = Mock(return_value=None)
        mock_s3.open.return_value = file_mock
        
        with patch('json.load', return_value=subsquare_data):
            result = should_we_vote.fn(
                network="polkadot",
                proposal_id=100,
                s3_bucket="test-bucket",
                endpoint_url="https://test.endpoint.com",
                access_key="test_key",
                secret_key="test_secret"
            )
            
            assert result is False


class TestCreateAndSignVoteTx:
    """Test transaction creation and signing"""
    
    def test_create_and_sign_vote_tx_aye(self):
        """Test creating and signing an Aye vote transaction"""
        # Generate fresh test keypair
        test_keypair = TestVoterData.generate_test_keypair()
        test_mnemonic = test_keypair.mnemonic
        
        # Mock Prefect Secret blocks
        mock_rpc_secret = Mock()
        mock_rpc_secret.get.return_value = "wss://test-rpc.polkadot.io"
        
        mock_mnemonic_secret = Mock()
        mock_mnemonic_secret.get.return_value = test_mnemonic
        
        # Mock SubstrateInterface
        mock_substrate = Mock()
        mock_substrate.__enter__ = Mock(return_value=mock_substrate)
        mock_substrate.__exit__ = Mock(return_value=None)
        
        # Mock composed calls
        mock_vote_call = {"call": "vote"}
        mock_proxy_call = {"call": "proxy"}
        mock_remark_call = {"call": "remark"}
        mock_batch_call = {"call": "batch"}
        
        mock_substrate.compose_call.side_effect = [
            mock_vote_call,
            mock_proxy_call,
            mock_remark_call,
            mock_batch_call
        ]
        
        # Mock extrinsic
        mock_extrinsic = Mock()
        mock_extrinsic.data = "0x1234567890abcdef"
        mock_substrate.create_signed_extrinsic.return_value = mock_extrinsic
        
        with patch('cybergov_voter.Secret') as mock_secret_class:
            mock_secret_class.load.side_effect = [mock_rpc_secret, mock_mnemonic_secret]
            
            with patch('cybergov_voter.SubstrateInterface', return_value=mock_substrate):
                with patch('cybergov_voter.Keypair.create_from_mnemonic', return_value=test_keypair):
                    with patch('cybergov_voter.create_vote_parameters') as mock_create_params:
                        mock_vote_params = {"Standard": {"vote": {"aye": True, "conviction": 1}, "balance": 10000000000}}
                        mock_create_params.return_value = mock_vote_params
                        
                        result = create_and_sign_vote_tx.fn(
                            proposal_id=100,
                            network="polkadot",
                            vote="Aye",
                            remark_text="test_remark_hash"
                        )
                        
                        assert result == "0x1234567890abcdef"
                        
                        # Verify vote parameters were created correctly
                        mock_create_params.assert_called_once_with("Aye", "polkadot")
                        
                        # Verify substrate calls
                        assert mock_substrate.compose_call.call_count == 4
                        mock_substrate.create_signed_extrinsic.assert_called_once_with(
                            call=mock_batch_call, keypair=test_keypair
                        )
    
    def test_create_and_sign_vote_tx_abstain(self):
        """Test creating and signing an Abstain vote transaction"""
        test_keypair = TestVoterData.generate_test_keypair()
        test_mnemonic = test_keypair.mnemonic
        
        mock_rpc_secret = Mock()
        mock_rpc_secret.get.return_value = "wss://test-rpc.kusama.io"
        
        mock_mnemonic_secret = Mock()
        mock_mnemonic_secret.get.return_value = test_mnemonic
        
        mock_substrate = Mock()
        mock_substrate.__enter__ = Mock(return_value=mock_substrate)
        mock_substrate.__exit__ = Mock(return_value=None)
        
        mock_extrinsic = Mock()
        mock_extrinsic.data = "0xabcdef1234567890"
        mock_substrate.create_signed_extrinsic.return_value = mock_extrinsic
        
        with patch('cybergov_voter.Secret') as mock_secret_class:
            mock_secret_class.load.side_effect = [mock_rpc_secret, mock_mnemonic_secret]
            
            with patch('cybergov_voter.SubstrateInterface', return_value=mock_substrate):
                with patch('cybergov_voter.Keypair.create_from_mnemonic', return_value=test_keypair):
                    with patch('cybergov_voter.create_vote_parameters') as mock_create_params:
                        mock_vote_params = {"SplitAbstain": {"aye": 0, "nay": 0, "abstain": 1000000000000}}
                        mock_create_params.return_value = mock_vote_params
                        
                        result = create_and_sign_vote_tx.fn(
                            proposal_id=200,
                            network="kusama",
                            vote="Abstain",
                            remark_text="test_abstain_hash"
                        )
                        
                        assert result == "0xabcdef1234567890"
                        mock_create_params.assert_called_once_with("Abstain", "kusama")
    
    def test_create_and_sign_vote_tx_keypair_error(self):
        """Test handling of keypair loading errors"""
        mock_rpc_secret = Mock()
        mock_rpc_secret.get.return_value = "wss://test-rpc.polkadot.io"
        
        mock_mnemonic_secret = Mock()
        mock_mnemonic_secret.get.side_effect = ValueError("Invalid mnemonic")
        
        mock_substrate = Mock()
        mock_substrate.__enter__ = Mock(return_value=mock_substrate)
        mock_substrate.__exit__ = Mock(return_value=None)
        
        with patch('cybergov_voter.Secret') as mock_secret_class:
            mock_secret_class.load.side_effect = [mock_rpc_secret, mock_mnemonic_secret]
            
            with patch('cybergov_voter.SubstrateInterface', return_value=mock_substrate):
                with pytest.raises(ValueError):
                    create_and_sign_vote_tx.fn(
                        proposal_id=100,
                        network="polkadot",
                        vote="Aye",
                        remark_text="test_hash"
                    )
