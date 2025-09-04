import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone


@pytest.fixture
def temp_workspace():
    """Create a temporary workspace directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Path(temp_dir)
        yield workspace


@pytest.fixture
def sample_analysis_data():
    """Sample analysis data for different voting scenarios."""
    return {
        # Aye votes
        "balthazar_aye": {
            "model_name": "openai/gpt-4o",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Aye",
            "confidence": None,
            "rationale": "This proposal aligns with Polkadot's strategic advantage and competitive positioning.",
            "raw_api_response": {}
        },
        "melchior_aye": {
            "model_name": "openrouter/google/gemini-2.5-pro-preview",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Aye",
            "confidence": None,
            "rationale": "This proposal will significantly boost ecosystem growth and developer activity.",
            "raw_api_response": {}
        },
        "caspar_aye": {
            "model_name": "openrouter/x-ai/grok-code-fast-1",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Aye",
            "confidence": None,
            "rationale": "Actually, this proposal ensures long-term sustainability and protocol resilience.",
            "raw_api_response": {}
        },
        
        # Nay votes
        "balthazar_nay": {
            "model_name": "openai/gpt-4o",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Nay",
            "confidence": None,
            "rationale": "This proposal creates strategic disadvantage and weakens competitive positioning.",
            "raw_api_response": {}
        },
        "melchior_nay": {
            "model_name": "openrouter/google/gemini-2.5-pro-preview",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Nay",
            "confidence": None,
            "rationale": "This proposal will harm ecosystem growth and reduce developer activity.",
            "raw_api_response": {}
        },
        "caspar_nay": {
            "model_name": "openrouter/x-ai/grok-code-fast-1",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Nay",
            "confidence": None,
            "rationale": "This proposal poses risks to long-term sustainability and protocol resilience.",
            "raw_api_response": {}
        },
        
        # Abstain votes
        "balthazar_abstain": {
            "model_name": "openai/gpt-4o",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Abstain",
            "confidence": None,
            "rationale": "Insufficient information to make a strategic decision.",
            "raw_api_response": {}
        },
        "melchior_abstain": {
            "model_name": "openrouter/google/gemini-2.5-pro-preview",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Abstain",
            "confidence": None,
            "rationale": "Insufficient data for ecosystem analysis.",
            "raw_api_response": {}
        },
        "caspar_abstain": {
            "model_name": "openrouter/x-ai/grok-code-fast-1",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "Abstain",
            "confidence": None,
            "rationale": "Cannot assess long-term impact with available information.",
            "raw_api_response": {}
        },
        
        # Case variations for testing
        "balthazar_aye_lowercase": {
            "model_name": "openai/gpt-4o",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "aye",
            "confidence": None,
            "rationale": "Strategic advantage confirmed.",
            "raw_api_response": {}
        },
        "melchior_aye_uppercase": {
            "model_name": "openrouter/google/gemini-2.5-pro-preview",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "decision": "AYE",
            "confidence": None,
            "rationale": "Ecosystem benefits are clear.",
            "raw_api_response": {}
        }
    }


@pytest.fixture
def create_analysis_files():
    """Factory function to create analysis files with given data."""
    def _create_files(workspace, file_data_mapping):
        """
        Create analysis files in the workspace.
        
        Args:
            workspace: Path to workspace directory
            file_data_mapping: Dict mapping filename to data dict
            
        Returns:
            List of Path objects for created files
        """
        analysis_files = []
        for filename, data in file_data_mapping.items():
            file_path = workspace / f"{filename}.json"
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            analysis_files.append(file_path)
        return analysis_files
    
    return _create_files


@pytest.fixture
def mock_proposal_data():
    """Mock proposal data for testing."""
    return {
        "proposal_id": "123",
        "network": "polkadot"
    }


@pytest.fixture
def mock_s3_filesystem():
    """Mock S3 filesystem for testing preflight checks."""
    from unittest.mock import MagicMock
    
    mock_s3 = MagicMock()
    
    # Default behavior - files exist
    mock_s3.exists.return_value = True
    
    # Mock file contents
    def mock_open_context(path, mode='r'):
        mock_file = MagicMock()
        if 'raw_subsquare_data.json' in path:
            mock_file.__enter__.return_value.read.return_value = json.dumps({
                "referendumIndex": 123,
                "title": "Test Proposal",
                "content": "This is a test proposal content",
                "proposer": "test_proposer"
            })
        elif 'content.md' in path:
            mock_file.__enter__.return_value.read.return_value = "# Test Proposal\n\nThis is test content."
        mock_file.__exit__ = MagicMock(return_value=None)
        return mock_file
    
    mock_s3.open.side_effect = mock_open_context
    
    # Mock download - creates local files
    def mock_download(s3_path, local_path):
        local_file = Path(local_path)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        if 'raw_subsquare_data.json' in s3_path:
            content = {
                "referendumIndex": 123,
                "title": "Test Proposal",
                "content": "This is a test proposal content",
                "proposer": "test_proposer"
            }
            with open(local_file, 'w') as f:
                json.dump(content, f)
        elif 'content.md' in s3_path:
            with open(local_file, 'w') as f:
                f.write("# Test Proposal\n\nThis is test content.")
    
    mock_s3.download.side_effect = mock_download
    
    return mock_s3


@pytest.fixture
def mock_system_prompts(temp_workspace):
    """Create mock system prompt files for testing."""
    prompts_dir = temp_workspace / "templates" / "system_prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    
    magi_models = ["balthazar", "caspar", "melchior"]
    prompt_files = []
    
    for model in magi_models:
        prompt_file = prompts_dir / f"{model}_system_prompt.md"
        with open(prompt_file, 'w') as f:
            f.write(f"# {model.title()} System Prompt\n\nThis is the system prompt for {model}.")
        prompt_files.append(prompt_file)
    
    return prompt_files


@pytest.fixture
def sample_raw_subsquare_data():
    """Sample raw subsquare data for testing."""
    return {
        "valid_data": {
            "referendumIndex": 123,
            "title": "Test Proposal",
            "content": "This is a test proposal content",
            "proposer": "test_proposer",
            "extra_field": "this is fine"
        },
        "missing_title": {
            "referendumIndex": 123,
            "content": "This is a test proposal content",
            "proposer": "test_proposer"
        },
        "missing_multiple": {
            "referendumIndex": 123
        },
        "empty_data": {}
    }


@pytest.fixture
def mock_s3_filesystem():
    """Mock S3 filesystem for testing preflight checks."""
    from unittest.mock import MagicMock
    
    mock_s3 = MagicMock()
    
    # Default behavior - files exist
    mock_s3.exists.return_value = True
    
    # Mock file contents
    def mock_open_context(path, mode='r'):
        mock_file = MagicMock()
        if 'raw_subsquare_data.json' in path:
            mock_file.__enter__.return_value.read.return_value = json.dumps({
                "referendumIndex": 123,
                "title": "Test Proposal",
                "content": "This is a test proposal content",
                "proposer": "test_proposer"
            })
        elif 'content.md' in path:
            mock_file.__enter__.return_value.read.return_value = "# Test Proposal\n\nThis is test content."
        mock_file.__exit__ = MagicMock(return_value=None)
        return mock_file
    
    mock_s3.open.side_effect = mock_open_context
    
    # Mock download - creates local files
    def mock_download(s3_path, local_path):
        local_file = Path(local_path)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        if 'raw_subsquare_data.json' in s3_path:
            content = {
                "referendumIndex": 123,
                "title": "Test Proposal",
                "content": "This is a test proposal content",
                "proposer": "test_proposer"
            }
            with open(local_file, 'w') as f:
                json.dump(content, f)
        elif 'content.md' in s3_path:
            with open(local_file, 'w') as f:
                f.write("# Test Proposal\n\nThis is test content.")
    
    mock_s3.download.side_effect = mock_download
    
    return mock_s3


@pytest.fixture
def mock_system_prompts(temp_workspace):
    """Create mock system prompt files for testing."""
    prompts_dir = temp_workspace / "templates" / "system_prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    
    magi_models = ["balthazar", "caspar", "melchior"]
    prompt_files = []
    
    for model in magi_models:
        prompt_file = prompts_dir / f"{model}_system_prompt.md"
        with open(prompt_file, 'w') as f:
            f.write(f"# {model.title()} System Prompt\n\nThis is the system prompt for {model}.")
        prompt_files.append(prompt_file)
    
    return prompt_files


@pytest.fixture
def sample_raw_subsquare_data():
    """Sample raw subsquare data for testing."""
    return {
        "valid_data": {
            "referendumIndex": 123,
            "title": "Test Proposal",
            "content": "This is a test proposal content",
            "proposer": "test_proposer",
            "extra_field": "this is fine"
        },
        "missing_title": {
            "referendumIndex": 123,
            "content": "This is a test proposal content",
            "proposer": "test_proposer"
        },
        "missing_multiple": {
            "referendumIndex": 123
        },
        "empty_data": {}
    }
