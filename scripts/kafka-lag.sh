#!/usr/bin/env bash
# Offsets and consumer lag for the stock topics, any time.
#   scripts/kafka-lag.sh                       the API's sync consumer + the e2e test group
#   scripts/kafka-lag.sh my-group my.topic     one group (topic optional)
#   WATCH=2 scripts/kafka-lag.sh               refresh every 2 seconds
# Lag = log-end offset - committed offset, per partition. See scripts/lib/kafka.sh for details.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib/common.sh
source scripts/lib/kafka.sh
load_env
trap cleanup_tmp EXIT

report() {
  if [[ $# -gt 0 ]]; then
    kafka_lag_report "$1" "${2:-}"
  else
    step "topics"
    for t in "$EVENTS_TOPIC" "$SYNC_TOPIC"; do
      log "$t log-end offset per partition, total $(kafka_offsets "$t" latest | tee "$RS_TMP/o" | sum_offsets):"
      awk '{ printf "p%-2s=%-8s", $1, $2 } NR % 6 == 0 { print "" } END { if (NR % 6) print "" }' "$RS_TMP/o" | indent
    done
    step "groups"
    kafka_lag_report "$APP_SYNC_GROUP" "$SYNC_TOPIC"
#    kafka_lag_report "$E2E_GROUP" "$EVENTS_TOPIC"
  fi
}

if [[ -n "${WATCH:-}" ]]; then
  while :; do clear; report "$@"; sleep "$WATCH"; done
else
  report "$@"
fi
