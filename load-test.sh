#!/usr/bin/env bash
# Load test for the ingestion endpoint (POST .../adjustments), then checks the events reach Kafka.
#
#   ./load-test.sh                    2000 writes, 20 parallel workers (bash + curl, no extra tools)
#   ./load-test.sh -n 10000 -c 50     bigger run
#   ./load-test.sh -t wrk -d 30s      use wrk for 30 seconds (needs wrk installed)
#   ./load-test.sh --reads -t ab      read load with ApacheBench (GET a stock level)
#   ./load-test.sh --hot              every write hits ONE item: shows row-lock queueing / 503 STOCK_BUSY
#
# Each write gets its own Idempotency-Key, so every request is a real write (and a Kafka event).
# Writes spread over LOAD_SKUS items (default 100) unless --hot.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=scripts/lib/common.sh
source scripts/lib/common.sh
# shellcheck source=scripts/lib/webauthn.sh
source scripts/lib/webauthn.sh
# shellcheck source=scripts/lib/keycloak.sh
source scripts/lib/keycloak.sh
# shellcheck source=scripts/lib/kafka.sh
source scripts/lib/kafka.sh

TOTAL=2000
CONC=200
TOOL=auto
DURATION=30s
READS=""
HOT=""
SKUS="${LOAD_SKUS:-100}"
MAX_ERR_PCT="${LOAD_MAX_ERROR_PCT:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n) TOTAL=$2; shift 2 ;;
    -c) CONC=$2; shift 2 ;;
    -t) TOOL=$2; shift 2 ;;
    -d) DURATION=$2; shift 2 ;;
    --reads) READS=1; shift ;;
    --hot) HOT=1; SKUS=1; shift ;;
    -h | --help) sed -n '2,14p' "$0"; exit 0 ;;
    *) die "unknown option $1" ;;
  esac
done

load_env
trap cleanup_tmp EXIT
require_cmd curl jq openssl awk sort

if [[ "$TOOL" == auto ]]; then
  if [[ -n "$READS" ]] && command -v ab >/dev/null 2>&1; then TOOL=ab
  elif command -v wrk >/dev/null 2>&1; then TOOL=wrk
  else TOOL=bash
  fi
fi
if [[ "$TOOL" == ab && -z "$READS" ]]; then
  # ab sends the same headers every time: one Idempotency-Key = one write + N-1 replays. Not a write test.
  warn "ab can't vary Idempotency-Key per request, using bash workers for writes"
  TOOL=bash
fi
command -v "$TOOL" >/dev/null 2>&1 || [[ "$TOOL" == bash ]] || die "$TOOL is not installed"

RUN="$(date +%s)"
LOC="STORE-001"
KAFKA_OK=""
kafka_ping 2>/dev/null && KAFKA_OK=1

# ---------------------------------------------------------------- token
step "Token"
if [[ "${LOAD_AS:-service}" == teammate ]]; then
  TOKEN="$(kc_passkey_login "${TEAMMATE_USERNAME:-alice}" "${TEAMMATE_BOOTSTRAP_PASSWORD:-Alice-Passkey-2026!}")" || die "passkey login"
else
  TOKEN="$(kc_client_token "${SERVICE_CLIENT_ID:-retail-stock-sync-service}" "${SERVICE_CLIENT_SECRET:-sync-service-local-secret}")" || die "service token"
fi
log "acting as $(jwt_payload "$TOKEN" | jq -r '.preferred_username // .azp'), token valid until $(jwt_payload "$TOKEN" | jq -r '.exp | todate')"

sku_of() { if [[ -n "$HOT" ]]; then echo "SKU-LOAD-HOT"; else printf 'SKU-LOAD-%03d' $(($1 % SKUS)); fi; }

if [[ -n "$READS" ]]; then
  # Make sure the item exists so reads return 200.
  api POST "/api/v1/stock/$(sku_of 0)/locations/$LOC/adjustments" "$TOKEN" '{"delta":1,"reason":"seed"}' "load-seed-$RUN" >/dev/null
fi

if [[ -n "$KAFKA_OK" && -z "$READS" ]]; then
  kafka_offsets "$EVENTS_TOPIC" latest >"$RS_TMP/end-before"
else
  [[ -z "$READS" ]] && warn "Kafka CLI not reachable ($(kafka_mode)): skipping the Kafka part"
fi

# ---------------------------------------------------------------- runners

# bash: CONC background workers, each one curl process running its share of requests over one
# keep-alive connection (curl -K config with one block per request). Writes "status seconds" lines.
run_bash() {
  local per=$(((TOTAL + CONC - 1) / CONC)) w i n sku tls=""
  [[ ${#CURL_TLS[@]} -gt 0 ]] && tls="insecure"
  for w in $(seq 1 "$CONC"); do
    {
      for i in $(seq 1 "$per"); do
        n=$(((w - 1) * per + i))
        ((n <= TOTAL)) || break
        sku="$(sku_of "$n")"
        echo "url = \"$API_URL/api/v1/stock/$sku/locations/$LOC$([[ -z "$READS" ]] && echo /adjustments)\""
        echo "header = \"Authorization: Bearer $TOKEN\""
        if [[ -z "$READS" ]]; then
          echo 'request = "POST"'
          echo 'header = "Content-Type: application/json"'
          echo "header = \"Idempotency-Key: load-$RUN-$n\""
          echo "data = \"{\\\"delta\\\":1,\\\"reason\\\":\\\"load\\\",\\\"reference\\\":\\\"LOAD-$RUN\\\"}\""
        fi
        echo 'output = "/dev/null"'
        echo 'write-out = "%{http_code} %{time_total}\n"'
        [[ -n "$tls" ]] && echo "$tls"
        echo next
      done
    } >"$RS_TMP/w$w.cfg"
  done
  log "bash runner: $CONC workers x ~$per requests"
  for w in $(seq 1 "$CONC"); do
    curl -s -K "$RS_TMP/w$w.cfg" >"$RS_TMP/w$w.out" 2>/dev/null &
  done
  wait
  cat "$RS_TMP"/w*.out >"$RS_TMP/results"
}

# wrk: duration based; a Lua script gives each request a fresh Idempotency-Key and a random item.
run_wrk() {
  local threads=$((CONC < 4 ? CONC : 4))
  cat >"$RS_TMP/load.lua" <<LUA
local counter, id = 0, 0
local tid = 0
function setup(thread) thread:set("id", tid); tid = tid + 1 end
function init(args) math.randomseed(os.time() + id * 7919) end
request = function()
  counter = counter + 1
  local sku = "$HOT" ~= "" and "SKU-LOAD-HOT" or string.format("SKU-LOAD-%03d", math.random(0, $SKUS - 1))
  local path = "/api/v1/stock/" .. sku .. "/locations/$LOC"
  if "$READS" ~= "" then return wrk.format("GET", path) end
  wrk.headers["Idempotency-Key"] = string.format("load-$RUN-%d-%d", id, counter)
  return wrk.format("POST", path .. "/adjustments", nil, '{"delta":1,"reason":"load","reference":"LOAD-$RUN"}')
end
done = function(summary, latency, requests)
  io.write(string.format("WRK requests=%d non2xx=%d socket_errors=%d duration_s=%.2f\n",
    summary.requests, summary.errors.status, summary.errors.connect + summary.errors.read + summary.errors.write + summary.errors.timeout,
    summary.duration / 1e6))
  for _, p in ipairs({50, 90, 95, 99}) do
    io.write(string.format("WRK p%d_ms=%.1f\n", p, latency:percentile(p) / 1000))
  end
  io.write(string.format("WRK max_ms=%.1f avg_ms=%.1f\n", latency.max / 1000, latency.mean / 1000))
end
LUA
  log "wrk: $threads threads, $CONC connections, $DURATION"
  wrk -t"$threads" -c"$CONC" -d"$DURATION" --latency -s "$RS_TMP/load.lua" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" "$API_URL" | tee "$RS_TMP/wrk.out" | indent
}

run_ab() {
  log "ab: $TOTAL GET requests, concurrency $CONC"
  ab -q -n "$TOTAL" -c "$CONC" -k -H "Authorization: Bearer $TOKEN" \
    "$API_URL/api/v1/stock/$(sku_of 0)/locations/$LOC" | tee "$RS_TMP/ab.out" |
    grep -E 'Complete requests|Failed requests|Non-2xx|Requests per second|Time per request|^ +(50|90|95|99|100)%' | indent
}

# ---------------------------------------------------------------- run
step "Load: $([[ -n "$READS" ]] && echo reads || echo writes) with $TOOL$([[ -n "$HOT" ]] && echo ', HOT item')"
t0="$(now_ms)"
"run_$TOOL"
t1="$(now_ms)"
elapsed_ms=$((t1 - t0 > 0 ? t1 - t0 : 1))

ok=0
errors=0
case "$TOOL" in
  bash)
    # Percentiles: sort latencies, take the value at rank ceil(p * count).
    sort -k2 -n "$RS_TMP/results" | awk -v ms="$elapsed_ms" '
      { code[$1]++; t[NR] = $2 * 1000; sum += $2 * 1000 }
      END {
        n = NR; if (!n) { print "no results"; exit }
        printf "requests=%d  time=%.1fs  throughput=%.0f req/s\n", n, ms / 1000, n * 1000 / ms
        printf "latency ms: min=%.1f avg=%.1f p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f\n", t[1], sum / n,
          t[int(n * .50 + .999)], t[int(n * .90 + .999)], t[int(n * .95 + .999)], t[int(n * .99 + .999)], t[n]
        printf "status codes:"; for (c in code) printf " %s=%d", c, code[c]; printf "\n"
      }' | tee "$RS_TMP/stats" | indent
    ok="$(awk '$1 ~ /^2/' "$RS_TMP/results" | grep -c . || true)"
    errors=$(($(grep -c . "$RS_TMP/results" || true) - ok))
    ;;
  wrk)
    total="$(sed -n 's/^WRK requests=\([0-9]*\).*/\1/p' "$RS_TMP/wrk.out")"
    errors="$(sed -n 's/.*non2xx=\([0-9]*\) socket_errors=\([0-9]*\).*/\1 \2/p' "$RS_TMP/wrk.out" | awk '{ print $1 + $2 }')"
    ok=$((${total:-0} - ${errors:-0}))
    ;;
  ab)
    total="$(sed -n 's/^Complete requests: *//p' "$RS_TMP/ab.out")"
    errors="$(awk '/^Failed requests:/ { f = $3 } /^Non-2xx responses:/ { x = $3 } END { print f + x }' "$RS_TMP/ab.out")"
    ok=$((${total:-0} - ${errors:-0}))
    ;;
esac
log "successful=$ok errors=$errors"
kurl "$API_URL/metrics" | grep -E '^retail_stock_(http_requests_shed_total|http_requests_in_flight|outbox_pending) ' | indent || true

# ---------------------------------------------------------------- did it reach Kafka?
if [[ -n "$KAFKA_OK" && -z "$READS" && "$ok" -gt 0 ]]; then
  step "Kafka: waiting for $ok events on $EVENTS_TOPIC"
  start="$(now_ms)"
  before="$(sum_offsets <"$RS_TMP/end-before")"
  while :; do
    kafka_offsets "$EVENTS_TOPIC" latest >"$RS_TMP/end-after"
    grew=$(($(sum_offsets <"$RS_TMP/end-after") - before))
    ((grew >= ok)) && break
    if (($(now_ms) - start > ${LOAD_DRAIN_TIMEOUT:-120} * 1000)); then
      warn "only $grew/$ok events in Kafka after ${LOAD_DRAIN_TIMEOUT:-120}s, the rest is still in the outbox"
      break
    fi
    sleep 1
  done
  drain_ms=$(($(now_ms) - t0))
  if ((grew >= ok)); then
    log "all $ok events in Kafka ${drain_ms}ms after the first request ($((grew * 1000 / drain_ms)) events/s end to end)"
  else
    log "$grew events in Kafka so far ($((grew * 1000 / drain_ms)) events/s end to end)"
  fi
  log "per partition (log-end before -> after):"
  join <(sort -k1,1 "$RS_TMP/end-before") <(sort -k1,1 "$RS_TMP/end-after") | sort -n |
    awk '{ printf "partition %2d  %8d -> %8d  (+%d)\n", $1, $2, $3, $3 - $2 }' | indent
fi

# ---------------------------------------------------------------- verdict
total_req=$((ok + errors))
err_pct=$((total_req > 0 ? errors * 100 / total_req : 100))
if [[ -n "$HOT" ]]; then
  log "hot mode: 503 STOCK_BUSY under contention is expected, not counted as failure"
  exit 0
fi
if ((err_pct > MAX_ERR_PCT)); then
  die "error rate ${err_pct}% is above ${MAX_ERR_PCT}%"
fi
printf '%sload test passed%s (error rate %d%%)\n' "$C_GREEN" "$C_OFF" "$err_pct" >&2
