#!/usr/bin/env bash
# Sends one absolute stock count to retail.stock.sync.v1 with the Kafka CLI.
# The API's consumer group applies it (deduplicated by eventId).
#   scripts/publish-sync.sh SKU-1001 STORE-001 42
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib/common.sh
source scripts/lib/kafka.sh
load_env
trap cleanup_tmp EXIT

SKU="${1:-SKU-1001}"
LOC="${2:-STORE-001}"
QTY="${3:-42}"
ID="$(new_uuid)"

jq -nc --arg id "$ID" --arg sku "$SKU" --arg loc "$LOC" --argjson qty "$QTY" --arg now "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '{eventId: $id, sku: $sku, locationId: $loc, onHand: $qty, source: "CLI", countedAt: $now}' |
  awk -v k="$SKU|$LOC" '{ print k "~" $0 }' | kafka_produce "$SYNC_TOPIC"
echo "published $ID: $SKU @ $LOC = $QTY"
