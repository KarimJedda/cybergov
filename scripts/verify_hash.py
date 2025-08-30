import hashlib
import sys
import argparse
import json

def verify_canonical_json_hash(file_path: str, expected_hash: str) -> None:
    """
    Calculates the canonical SHA-256 hash of a JSON file and compares it.

    This function first parses the JSON file, then dumps it back to a string
    with sorted keys and no extraneous whitespace. This ensures that the hash
    is based on the file's data content, not its specific formatting.
    """
    print(f"--- Verifying Canonical JSON Hash for: {file_path} ---")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"\n❌ Error: File not found at '{file_path}'")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"\n❌ Error: File '{file_path}' is not a valid JSON file.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: Could not read file. Reason: {e}")
        sys.exit(1)

    # 1. Create the canonical representation of the JSON data.
    #    - sort_keys=True: Ensures key order is always the same.
    #    - separators=(',', ':'): Removes all non-essential whitespace.
    canonical_string = json.dumps(data, sort_keys=True, separators=(',', ':'))

    # 2. Encode the canonical string into bytes for hashing.
    canonical_bytes = canonical_string.encode('utf-8')

    # 3. Calculate the hash of the canonical bytes.
    calculated_hash = hashlib.sha256(canonical_bytes).hexdigest()

    # Normalize for a case-insensitive comparison.
    is_match = calculated_hash.lower() == expected_hash.lower()

    print(f"Expected Hash:   {expected_hash.lower()}")
    print(f"Calculated Hash: {calculated_hash.lower()}")

    if is_match:
        print("\n✅ Success: The canonical hash matches the expected hash.")
        sys.exit(0)
    else:
        print("\n❌ Failure: Hashes DO NOT match! The file's content may differ.")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verify the canonical SHA-256 hash of a JSON file.",
        epilog="Example: python verify_canonical_hash.py ./manifest.json 5e884898da28..."
    )
    parser.add_argument("file_path", type=str, help="Path to the JSON file to verify.")
    parser.add_argument("expected_hash", type=str, help="The expected SHA-256 hash string.")
    args = parser.parse_args()
    
    verify_canonical_json_hash(args.file_path, args.expected_hash)