# shellcheck shell=bash
# shellcheck disable=SC2034  # variables here are read by the scripts that source this file
# Shared helpers for the test scripts: env, logging, HTTP calls, checks.
# Written for bash 3.2 too (the one macOS ships), so no associative arrays or mapfile.

RS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RS_STATE_DIR="${RS_STATE_DIR:-$RS_ROOT/.e2e}"

# ---------------------------------------------------------------- logging

if [[ -t 2 && -z "${NO_COLOR:-}" ]]; then
  C_OFF=$'\033[0m' C_DIM=$'\033[2m' C_BOLD=$'\033[1m'
  C_RED=$'\033[31m' C_GREEN=$'\033[32m' C_YELLOW=$'\033[33m' C_CYAN=$'\033[36m'
else
  C_OFF='' C_DIM='' C_BOLD='' C_RED='' C_GREEN='' C_YELLOW='' C_CYAN=''
fi

# Everything goes to stderr so functions can still print results on stdout.
log()  { printf '%s%s%s %s\n' "$C_DIM" "$(date +%H:%M:%S)" "$C_OFF" "$*" >&2; }
info() { printf '%s%s%s %s%s%s\n' "$C_DIM" "$(date +%H:%M:%S)" "$C_OFF" "$C_CYAN" "$*" "$C_OFF" >&2; }
warn() { printf '%s%s WARN%s %s\n' "$C_YELLOW" "$(date +%H:%M:%S)" "$C_OFF" "$*" >&2; }
die()  { printf '%s%s FAIL%s %s\n' "$C_RED" "$(date +%H:%M:%S)" "$C_OFF" "$*" >&2; exit 1; }
step() { printf '\n%s== %s ==%s\n' "$C_BOLD" "$*" "$C_OFF" >&2; }
indent() { sed 's/^/    /' >&2; }

# ---------------------------------------------------------------- checks

PASS=0
FAIL=0

# check <name> <expected> <actual>
check() {
  if [[ "$2" == "$3" ]]; then
    PASS=$((PASS + 1))
    printf '  %sok%s   %s (%s)\n' "$C_GREEN" "$C_OFF" "$1" "$3" >&2
  else
    FAIL=$((FAIL + 1))
    printf '  %sFAIL%s %s: expected %s, got %s\n' "$C_RED" "$C_OFF" "$1" "$2" "$3" >&2
  fi
}

summary() {
  printf '\n%spassed=%d failed=%d%s\n' "$C_BOLD" "$PASS" "$FAIL" "$C_OFF" >&2
  [[ "$FAIL" -eq 0 ]]
}

require_cmd() {
  local c missing=()
  for c in "$@"; do command -v "$c" >/dev/null 2>&1 || missing+=("$c"); done
  [[ ${#missing[@]} -eq 0 ]] || die "missing tools: ${missing[*]}"
}

# ---------------------------------------------------------------- env

# Loads .env, then picks the targets. K8S=1 switches to the minikube ingress hosts.
# Anything already exported (API_URL, KEYCLOAK_URL, ...) wins.
load_env() {
  local api="${API_URL:-}" kc="${KEYCLOAK_URL:-}"
  if [[ -f "$RS_ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$RS_ROOT/.env"
    set +a
  fi
  if [[ -n "${K8S:-}" ]]; then
    API_URL="${api:-https://stock.local}"
    KEYCLOAK_URL="${kc:-https://auth.stock.local}"
    CURL_TLS=(-k) # self-signed certs on minikube
  else
    API_URL="${api:-http://localhost:${APP_PORT:-8080}}"
    KEYCLOAK_URL="${kc:-http://localhost:${KEYCLOAK_HOST_PORT:-8180}}"
    CURL_TLS=()
  fi
  REALM="${REALM:-retail-stock}"
  ISSUER="$KEYCLOAK_URL/realms/$REALM"
  RS_TMP="$(mktemp -d "${TMPDIR:-/tmp}/rs-e2e.XXXXXX")"
  mkdir -p "$RS_STATE_DIR"
}

cleanup_tmp() { [[ -n "${RS_TMP:-}" && -d "$RS_TMP" ]] && rm -rf "$RS_TMP"; return 0; }

# curl with the TLS flags for the current target.
kurl() { curl -sS ${CURL_TLS[@]+"${CURL_TLS[@]}"} "$@"; }

# ---------------------------------------------------------------- encoding

b64url_encode() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

b64url_decode() {
  local s
  s="$(tr -- '-_' '+/')"
  case $((${#s} % 4)) in 2) s="$s==" ;; 3) s="$s=" ;; esac
  printf '%s' "$s" | openssl base64 -d -A
}

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr '[:upper:]' '[:lower:]'
  elif [[ -r /proc/sys/kernel/random/uuid ]]; then
    cat /proc/sys/kernel/random/uuid
  else
    openssl rand -hex 16 | sed -E 's/(.{8})(.{4})(.{4})(.{4})(.{12})/\1-\2-\3-\4-\5/'
  fi
}

# Prints the JSON payload of a JWT (no signature check, display only).
jwt_payload() { printf '%s' "$1" | cut -d. -f2 | b64url_decode; }

# Current time in milliseconds (macOS date has no %N, perl ships with macOS).
now_ms() {
  local ms
  ms="$(date +%s%3N 2>/dev/null)"
  if [[ "$ms" =~ ^[0-9]+$ ]]; then echo "$ms"
  elif command -v perl >/dev/null 2>&1; then perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
  else echo "$(($(date +%s) * 1000))"
  fi
}

# ---------------------------------------------------------------- HTTP

# api <METHOD> <PATH> <TOKEN> [JSON_BODY] [IDEMPOTENCY_KEY]
# Prints the status code. The body lands in $RS_TMP/body (see `body`).
api() {
  local method=$1 path=$2 token=$3 data=${4:-} key=${5:-}
  local args=(-o "$RS_TMP/body" -w '%{http_code}' -X "$method" "$API_URL$path")
  [[ -n "$token" ]] && args+=(-H "Authorization: Bearer $token")
  [[ -n "$data" ]] && args+=(-H 'Content-Type: application/json' -d "$data")
  [[ -n "$key" ]] && args+=(-H "Idempotency-Key: $key")
  kurl "${args[@]}" || echo 000
}

body() { cat "$RS_TMP/body"; }

# wait_for <name> <url> [timeout_seconds]
wait_for() {
  local name=$1 url=$2 timeout=${3:-120} start
  start=$(date +%s)
  until kurl -f -o /dev/null "$url" 2>/dev/null; do
    if (($(date +%s) - start > timeout)); then die "$name not reachable at $url after ${timeout}s"; fi
    sleep 2
  done
  log "$name is up ($url)"
}
