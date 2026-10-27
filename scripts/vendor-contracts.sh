#!/usr/bin/env bash
# Re-vendors contracts/ from local clones of data-contracts and telemetry-parity at the SHAs in
# contracts/SOURCE. Usage: scripts/vendor-contracts.sh <data-contracts dir> <telemetry-parity dir>
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
dc="$(cd "$1" && pwd)"
tp="$(cd "$2" && pwd)"
# shellcheck disable=SC1091
source "$root/contracts/SOURCE"
git -C "$dc" checkout -q "$DATA_CONTRACTS_SHA"
git -C "$tp" checkout -q "$TELEMETRY_PARITY_SHA"
cd "$root/contracts"
find . -mindepth 1 -maxdepth 1 ! -name SOURCE -exec rm -rf {} +
cp "$dc/contracts/common.schema.json" "$dc/contracts/topics.yaml" .
cp -r "$dc/contracts/topics" topics
mkdir -p fixtures golden
cp -r "$dc/fixtures/sample" fixtures/sample
for job in ota-status-tracker charging-session-builder; do
  cp -r "$tp/golden/sample/$job" "golden/$job"
done
echo "vendored data-contracts@$DATA_CONTRACTS_SHA telemetry-parity@$TELEMETRY_PARITY_SHA"
