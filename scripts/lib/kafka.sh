# shellcheck shell=bash
# shellcheck disable=SC2034  # variables here are read by the scripts that source this file
# Kafka helpers built on the stock Kafka CLI tools, run inside the broker container/pod.
# Needs common.sh.
#
# Offsets, in one picture (one partition):
#
#   log-start            committed (CURRENT-OFFSET)          log-end (LOG-END-OFFSET)
#      |-------- read ---------|------------ not read yet ------------|
#      0                       41                                     44
#
#   LOG-END-OFFSET  offset the *next* record will get (= records ever written to the partition)
#   CURRENT-OFFSET  next offset the group will read (what it committed, not the last one it read)
#   LAG             LOG-END-OFFSET - CURRENT-OFFSET = records written but not consumed yet (3 here)
#   Group lag = sum of the partition lags. "-" = the group never committed on that partition.
#
# Where the CLI runs:
#   KAFKA_CLI=docker (default)  docker compose exec kafka, bootstrap kafka:19092 (internal listener)
#   KAFKA_CLI=k8s    (K8S=1)    kubectl exec into the Strimzi broker pod, bootstrap localhost:9092
#   KAFKA_CLI=local             tools from $KAFKA_BIN on this machine, bootstrap localhost:9092

EVENTS_TOPIC="${KAFKA_EVENTS_TOPIC:-retail.stock.events.v1}"
SYNC_TOPIC="${KAFKA_SYNC_TOPIC:-retail.stock.sync.v1}"
APP_SYNC_GROUP="${KAFKA_CONSUMER_GROUP:-retail-stock-sync}"
E2E_GROUP="${E2E_GROUP:-retail-stock-e2e}"

kafka_mode() {
  if [[ -n "${KAFKA_CLI:-}" ]]; then echo "$KAFKA_CLI"
  elif [[ -n "${K8S:-}" ]]; then echo k8s
  else echo docker
  fi
}

_kafka_pod() {
  local pod
  pod="$(kubectl -n retail-stock-data get pod -l strimzi.io/cluster=retail-stock-kafka,strimzi.io/broker-role=true \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  echo "${pod:-retail-stock-kafka-dual-role-0}"
}

# kafka_exec <tool.sh> [args...]: runs a Kafka CLI tool against the broker (stdin is passed through).
kafka_exec() {
  local tool=$1
  shift
  case "$(kafka_mode)" in
    docker)
      docker compose -f "$RS_ROOT/docker-compose.yml" --env-file "$RS_ROOT/.env" exec -T kafka \
        "/opt/kafka/bin/$tool" --bootstrap-server localhost:19092 "$@"
      ;;
    k8s)
      kubectl -n retail-stock-data exec -i "$(_kafka_pod)" -c kafka -- \
        "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"
      ;;
    local)
      local bin="${KAFKA_BIN:-}"
      local cmd="${bin:+$bin/}$tool"
      command -v "$cmd" >/dev/null 2>&1 || cmd="${bin:+$bin/}${tool%.sh}" # Homebrew drops the .sh
      "$cmd" --bootstrap-server "${KAFKA_LOCAL_BOOTSTRAP:-localhost:9092}" "$@"
      ;;
    *) die "unknown KAFKA_CLI=$(kafka_mode) (docker|k8s|local)" ;;
  esac
}

kafka_ping() { kafka_exec kafka-topics.sh --list >/dev/null 2>&1; }

# ---------------------------------------------------------------- offsets

# kafka_offsets <topic> <latest|earliest>  -> "partition offset" lines, sorted by partition
# kafka-get-offsets prints "topic:partition:offset". latest = log-end, earliest = log-start.
kafka_offsets() {
  local t=-1
  [[ "$2" == earliest ]] && t=-2
  kafka_exec kafka-get-offsets.sh --topic "$1" --time "$t" 2>/dev/null |
    awk -F: 'NF >= 3 { print $(NF-1), $NF }' | sort -n
}

# Sum of the second column.
sum_offsets() { awk '{ s += $2 } END { print s + 0 }'; }

# ---------------------------------------------------------------- produce / consume

# kafka_produce <topic>: reads "key~json" lines on stdin.
kafka_produce() {
  kafka_exec kafka-console-producer.sh --topic "$1" \
    --property parse.key=true --property key.separator='~' >/dev/null 2>"$RS_TMP/producer.err" ||
    { cat "$RS_TMP/producer.err" | indent; return 1; }
}

# kafka_consume <topic> <group> <max_messages> [timeout_ms]
# Prints one tab-separated line per record: timestamp, partition, offset, headers, key, value.
# The console consumer commits the group's offsets when it exits.
kafka_consume() {
  kafka_exec kafka-console-consumer.sh --topic "$1" --group "$2" --from-beginning \
    --max-messages "$3" --timeout-ms "${4:-20000}" \
    --property print.timestamp=true --property print.partition=true --property print.offset=true \
    --property print.headers=true --property print.key=true 2>"$RS_TMP/consumer.err" || true
}

# Moves a group (must have no running members) to the end of a topic, so it only sees new records.
kafka_group_to_latest() {
  kafka_exec kafka-consumer-groups.sh --group "$1" --topic "$2" --reset-offsets --to-latest --execute >/dev/null 2>&1
}

# ---------------------------------------------------------------- lag report

# kafka_describe_group <group>: raw `kafka-consumer-groups --describe` output (saved for the report).
kafka_describe_group() {
  kafka_exec kafka-consumer-groups.sh --describe --group "$1" 2>&1 | tee "$RS_TMP/describe-$1.txt"
}

# kafka_group_lag <group> [topic]  -> prints the total lag (number), or -1 if the group has no offsets.
kafka_group_lag() {
  kafka_exec kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null | _lag_table "${2:-}" | awk '/^TOTAL/ { print $5; f=1 } END { if (!f) print -1 }'
}

# _lag_table [topic]: turns describe output into "partition committed log-end lag consumer" rows.
# Columns are found by header name, so extra columns in newer Kafka versions don't break it.
# Lag is recomputed as LOG-END-OFFSET - CURRENT-OFFSET and checked against Kafka's own LAG column.
_lag_table() {
  awk -v want="${1:-}" '
    $1 == "GROUP" && $2 == "TOPIC" {
      for (i = 1; i <= NF; i++) col[$i] = i
      next
    }
    !col["TOPIC"] || NF < 6 { next }
    want != "" && $col["TOPIC"] != want { next }
    {
      cur = $col["CURRENT-OFFSET"]; end = $col["LOG-END-OFFSET"]; tool = $col["LAG"]
      member = (col["CONSUMER-ID"] && $col["CONSUMER-ID"] != "-") ? $col["CONSUMER-ID"] : "-"
      if (cur ~ /^[0-9]+$/ && end ~ /^[0-9]+$/) {
        lag = end - cur
        if (tool ~ /^[0-9]+$/ && tool != lag) lag = lag "!=" tool   # should never happen
        tc += cur; te += end; tl += end - cur
      } else {
        lag = "-"                                                   # nothing committed yet
        if (end ~ /^[0-9]+$/) te += end
      }
      printf "%s %s %s %s %s %s\n", $col["TOPIC"], $col["PARTITION"], cur, end, lag, member
      rows++
    }
    END { if (rows) printf "TOTAL - %d %d %d -\n", tc, te, tl }
  '
}

# kafka_lag_report <group> [topic]: the detailed, human readable view.
#   - group state and members
#   - per partition: log-start, committed (CURRENT-OFFSET), log-end, lag, consumer
#   - totals, plus what the numbers mean
kafka_lag_report() {
  local group=$1 topic=${2:-} raw state starts
  raw="$(kafka_exec kafka-consumer-groups.sh --describe --group "$group" 2>&1)"
  state="$(kafka_exec kafka-consumer-groups.sh --describe --group "$group" --state 2>/dev/null |
    awk '$1 == "GROUP" { h = 1; next } h && NF { print $(NF-1), "members=" $NF }')"  # STATE and #MEMBERS are the last two columns

  info "consumer group '$group'${topic:+ on $topic}: state=${state:-unknown}"
  if printf '%s' "$raw" | grep -q 'does not exist'; then
    log "  group has never committed offsets"
    return 0
  fi
  log "  raw kafka-consumer-groups --describe:"
  printf '%s\n' "$raw" | grep -v '^$' | indent

  # Log-start offsets (oldest retained record) per partition, to show how much is still on disk.
  [[ -n "$topic" ]] && starts="$(kafka_offsets "$topic" earliest)"

  log "  computed (lag = log-end - committed):"
  {
    printf '%-26s %4s %10s %10s %10s %8s  %s\n' TOPIC PART LOG-START COMMITTED LOG-END LAG CONSUMER
    printf '%s\n' "$raw" | _lag_table "$topic" | while read -r t p cur end lag member; do
      if [[ "$t" == TOTAL ]]; then
        printf '%-26s %4s %10s %10s %10s %8s\n' TOTAL '' '' "$cur" "$end" "$lag"
        printf '=> lag %s: %s records written to %s that this group has not committed yet\n' "$lag" "$lag" "${topic:-the topic}"
      else
        local start="-"
        [[ -n "${starts:-}" ]] && start="$(printf '%s\n' "$starts" | awk -v p="$p" '$1 == p { print $2 }')"
        printf '%-26s %4s %10s %10s %10s %8s  %s\n' "$t" "$p" "${start:--}" "$cur" "$end" "$lag" "${member:0:40}"
      fi
    done
  } | indent
}

# kafka_wait_lag_zero <group> <topic> [timeout_s]: waits until the group caught up.
kafka_wait_lag_zero() {
  local group=$1 topic=$2 timeout=${3:-60} start lag
  start=$(date +%s)
  while :; do
    lag="$(kafka_group_lag "$group" "$topic")"
    [[ "$lag" == 0 ]] && return 0
    if (($(date +%s) - start > timeout)); then warn "$group still has lag=$lag after ${timeout}s"; return 1; fi
    log "  $group lag=$lag, waiting..."
    sleep 2
  done
}
