#!/usr/bin/env bash
# Prints an access token from Keycloak.
#   scripts/token.sh teammate   alice, passkey login (software passkey in .e2e/, enrolled on first use)
#   scripts/token.sh partner    partner-acme, client credentials (STORE-001/002, read + reserve)
#   scripts/token.sh service    retail-stock-sync-service, client credentials (all locations)
#   scripts/token.sh dev        dev.ops, password grant on retail-stock-dev-cli (local only)
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib/common.sh
source scripts/lib/webauthn.sh
source scripts/lib/keycloak.sh
load_env
trap cleanup_tmp EXIT

case "${1:-teammate}" in
  teammate) kc_passkey_login "${TEAMMATE_USERNAME:-alice}" "${TEAMMATE_BOOTSTRAP_PASSWORD:-Alice-Passkey-2026!}" 2>/dev/null ;;
  partner) kc_client_token "${PARTNER_CLIENT_ID:-partner-acme}" "${PARTNER_CLIENT_SECRET:-partner-acme-local-secret}" ;;
  service) kc_client_token "${SERVICE_CLIENT_ID:-retail-stock-sync-service}" "${SERVICE_CLIENT_SECRET:-sync-service-local-secret}" ;;
  dev)
    kurl -X POST "$ISSUER/protocol/openid-connect/token" -d grant_type=password -d client_id=retail-stock-dev-cli \
      -d scope=openid --data-urlencode username="${DEV_TEAMMATE_USERNAME:-dev.ops}" \
      --data-urlencode password="${DEV_TEAMMATE_PASSWORD:-dev-ops-local-pwd}" | jq -er .access_token
    ;;
  *) die "usage: $0 [teammate|partner|service|dev]" ;;
esac
