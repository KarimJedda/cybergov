#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
# Treat unset variables as an error.
# Fail a pipeline if any command in it fails.
set -euo pipefail


PREFECT_PORT=4200

# --- Script ---

echo "Verifying that required environment variables are set..."
if [ -z "${SSH_PRIVATE_KEY}" ] || [ -z "${SSH_USER}" ] || [ -z "${SSH_HOST}" ]; then
  echo "Error: Required environment variables (SSH_PRIVATE_KEY, SSH_USER, SSH_HOST) are not set."
  exit 1
fi

KEY_FILE_PATH=$(mktemp)

trap 'rm -f "$KEY_FILE_PATH"' EXIT

echo "Writing SSH private key to a temporary file..."
echo "${SSH_PRIVATE_KEY}" > "${KEY_FILE_PATH}"
chmod 600 "${KEY_FILE_PATH}"

echo "Establishing SSH tunnel to bastion in the background..."
ssh -i "${KEY_FILE_PATH}" \
    -L "${PREFECT_PORT}:localhost:${PREFECT_PORT}" \
    -f -N \
    "${SSH_USER}@${SSH_HOST}" \
    -o StrictHostKeyChecking=no

echo "Tunnel command executed. Waiting for 5 seconds for the connection to establish..."
sleep 5

echo "Running test against the local forwarded port (${PREFECT_PORT})..."
curl --fail --silent --show-error "http://localhost:${PREFECT_PORT}"

echo "Successfully connected to Prefect through the SSH tunnel."