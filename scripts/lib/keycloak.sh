# shellcheck shell=bash
# Keycloak helpers: client-credentials tokens, and a teammate passkey login done with plain curl.
# Needs common.sh and webauthn.sh.
#
# Passkey login = OIDC authorization code + PKCE on the public client `retail-stock-web`:
#   1. GET  /auth            -> login page with a WebAuthn challenge (realm has passkeys enabled)
#   2. sign the challenge     -> wa_assert (software passkey)
#   3. POST the webauth form  -> 302 to <redirect_uri>?code=...
#   4. POST /token            -> code + PKCE verifier = access token
# First time only, the user enrolls: password once, then Keycloak's `webauthn-register-passwordless`
# required action receives our public key (wa_register).

# ---------------------------------------------------------------- tokens

# kc_client_token <client_id> <secret>  -> prints an access token (partners, services)
kc_client_token() {
  local res token
  res="$(kurl -X POST "$ISSUER/protocol/openid-connect/token" \
    -d grant_type=client_credentials --data-urlencode client_id="$1" --data-urlencode client_secret="$2")"
  token="$(printf '%s' "$res" | jq -r '.access_token // empty' 2>/dev/null)"
  [[ -n "$token" ]] || { warn "client_credentials for $1 failed: $res"; return 1; }
  printf '%s\n' "$token"
}

# show_token <label> <token>: prints the claims the API cares about.
show_token() {
  log "$1 token claims:"
  jwt_payload "$2" | jq -c '{azp, user: (.preferred_username // .clientId), roles: [.realm_access.roles[]? | select(startswith("stock:") or . == "teammate" or . == "partner" or . == "service")], location_groups, expires: (.exp | todate)}' | indent
}

# ---------------------------------------------------------------- admin (best effort)

kc_admin_token() {
  kurl -X POST "$KEYCLOAK_URL/realms/master/protocol/openid-connect/token" \
    -d grant_type=password -d client_id=admin-cli \
    --data-urlencode username="${KEYCLOAK_ADMIN_USER:-admin}" \
    --data-urlencode password="${KEYCLOAK_ADMIN_PASSWORD:-admin_local_pwd}" | jq -r '.access_token // empty'
}

# Removes the user's passkeys in Keycloak and asks for a new one on next login.
# Used when we have no local key: the old passkey (from a browser, or a wiped .e2e/) is useless to us.
kc_admin_reset_passkey() {
  local user=$1 admin id cred
  admin="$(kc_admin_token)"
  [[ -n "$admin" ]] || { warn "no admin token, cannot reset $user's passkeys"; return 1; }
  local base="$KEYCLOAK_URL/admin/realms/$REALM/users"
  id="$(kurl -H "Authorization: Bearer $admin" "$base?username=$user&exact=true" | jq -r '.[0].id // empty')"
  [[ -n "$id" ]] || { warn "user $user not found in realm $REALM"; return 1; }

  for cred in $(kurl -H "Authorization: Bearer $admin" "$base/$id/credentials" |
    jq -r '.[] | select(.type == "webauthn-passwordless") | .id'); do
    kurl -X DELETE -H "Authorization: Bearer $admin" "$base/$id/credentials/$cred" >/dev/null
    log "deleted old passkey $cred for $user"
  done

  kurl -H "Authorization: Bearer $admin" "$base/$id" |
    jq -c '{requiredActions: ((.requiredActions // []) + ["webauthn-register-passwordless"] | unique)}' |
    kurl -X PUT -H "Authorization: Bearer $admin" -H 'Content-Type: application/json' -d @- "$base/$id" >/dev/null
  log "$user will be asked to enroll a passkey"
}

# ---------------------------------------------------------------- HTML scraping

# html_form_action <file> <form id>
html_form_action() {
  grep -o "<form[^>]*id=\"$2\"[^>]*>" "$1" | head -1 | sed -n 's/.*action="\([^"]*\)".*/\1/p' | sed 's/&amp;/\&/g'
}

# js_field <file> <name>: reads `name : "value"` from the inline script Keycloak renders.
js_field() {
  sed -n "s/.*[^A-Za-z]$2 *: *[\"']\([^\"']*\)[\"'].*/\1/p" "$1" | head -1
}

# Error text on a Keycloak page, if any.
kc_page_error() {
  grep -oE '(kc-feedback-text|pf-v5-c-alert__title|input-error)[^>]*>[^<]+' "$1" 2>/dev/null |
    sed 's/.*>//' | head -1
}

origin_of() { printf '%s' "$1" | sed -E 's#^(https?://[^/]+).*#\1#'; }

query_param() { printf '%s' "$1" | sed -n "s/.*[?&]$2=\([^&#]*\).*/\1/p"; }

# ---------------------------------------------------------------- browser-ish flow

# _kc_post <url> <out_page> <curl data args...>  -> sets KC_STATUS and KC_LOCATION
_kc_post() {
  local url=$1 out=$2 res
  shift 2
  res="$(kurl -b "$KC_JAR" -c "$KC_JAR" -o "$out" -w '%{http_code} %{redirect_url}' -X POST "$url" "$@")"
  KC_STATUS="${res%% *}"
  KC_LOCATION="${res#* }"
}

# _kc_start: fresh cookie jar + PKCE, GET the authorize endpoint. Login page -> $RS_TMP/login.html
_kc_start() {
  KC_JAR="$RS_TMP/cookies"
  rm -f "$KC_JAR"
  KC_VERIFIER="$(openssl rand 48 | b64url_encode)"
  KC_STATE="$(openssl rand -hex 12)"
  local pkce
  pkce="$(printf '%s' "$KC_VERIFIER" | openssl dgst -sha256 -binary | b64url_encode)"
  kurl -b "$KC_JAR" -c "$KC_JAR" -o "$RS_TMP/login.html" -G "$ISSUER/protocol/openid-connect/auth" \
    --data-urlencode client_id="$KC_CLIENT" --data-urlencode redirect_uri="$KC_REDIRECT" \
    -d response_type=code -d scope=openid -d state="$KC_STATE" \
    -d code_challenge="$pkce" -d code_challenge_method=S256
}

# kc_enroll_passkey <user> <password> <dir>: password once, then register the software passkey.
kc_enroll_passkey() {
  local user=$1 password=$2 dir=$3 action challenge rp_id user_handle
  kc_admin_reset_passkey "$user" || warn "continuing without reset"
  _kc_start
  action="$(html_form_action "$RS_TMP/login.html" kc-form-login)"
  [[ -n "$action" ]] || { warn "no login form from Keycloak"; return 1; }

  log "enroll 1/3: password login for $user (bootstrap credential, used once)"
  _kc_post "$action" "$RS_TMP/after-password.html" --data-urlencode username="$user" --data-urlencode password="$password"
  case "$KC_LOCATION" in
    *required-action*webauthn-register*) ;;
    *code=*) warn "Keycloak did not ask for a passkey (user already has one?)"; return 1 ;;
    *) warn "password login failed ($KC_STATUS): $(kc_page_error "$RS_TMP/after-password.html")"; return 1 ;;
  esac

  kurl -b "$KC_JAR" -c "$KC_JAR" -o "$RS_TMP/register.html" "$KC_LOCATION"
  action="$(html_form_action "$RS_TMP/register.html" register)"
  challenge="$(js_field "$RS_TMP/register.html" challenge)"
  rp_id="$(js_field "$RS_TMP/register.html" rpId)"
  user_handle="$(js_field "$RS_TMP/register.html" userid)"
  [[ -n "$action" && -n "$challenge" && -n "$user_handle" ]] || { warn "could not read the passkey registration page"; return 1; }
  log "enroll 2/3: registration challenge=$challenge rpId=$rp_id origin=$(origin_of "$action")"

  wa_register "$dir" "$challenge" "$rp_id" "$(origin_of "$action")" "$user_handle"
  _kc_post "$action" "$RS_TMP/after-register.html" \
    --data-urlencode clientDataJSON="$WA_CLIENT_DATA" \
    --data-urlencode attestationObject="$WA_ATTESTATION" \
    --data-urlencode publicKeyCredentialId="$WA_CRED_ID" \
    --data-urlencode authenticatorLabel="bash e2e ($(hostname -s 2>/dev/null || echo host))" \
    --data-urlencode transports=internal \
    --data-urlencode authenticatorAttachment=platform \
    -d error=
  if [[ "$KC_LOCATION" != *code=* ]]; then
    warn "passkey registration rejected ($KC_STATUS): $(kc_page_error "$RS_TMP/after-register.html")"
    wa_forget "$dir"
    return 1
  fi
  log "enroll 3/3: passkey registered, credentialId=$WA_CRED_ID"
}

# kc_passkey_login <user> <bootstrap_password>  -> prints an access token
kc_passkey_login() {
  local user=$1 password=$2 dir action challenge rp_id origin code res token attempt
  KC_CLIENT="${PASSKEY_CLIENT_ID:-retail-stock-web}"
  KC_REDIRECT="${PASSKEY_REDIRECT_URI:-$API_URL/callback}"
  dir="$(wa_dir "$user")"

  for attempt in 1 2; do
    if ! wa_has_credential "$dir"; then
      log "no local passkey for $user, enrolling one"
      kc_enroll_passkey "$user" "$password" "$dir" || return 1
    fi

    _kc_start
    action="$(html_form_action "$RS_TMP/login.html" webauth)"
    challenge="$(js_field "$RS_TMP/login.html" challenge)"
    rp_id="$(js_field "$RS_TMP/login.html" rpId)"
    [[ -n "$action" && -n "$challenge" ]] || { warn "login page has no passkey form (is webAuthnPolicyPasswordlessPasskeysEnabled on?)"; return 1; }
    origin="$(origin_of "$action")"
    log "passkey 1/3: challenge=$challenge rpId=$rp_id origin=$origin"

    wa_assert "$dir" "$challenge" "$rp_id" "$origin"
    log "passkey 2/3: signed with credentialId=$WA_CRED_ID signCount=$(cat "$dir/sign_count")"
    _kc_post "$action" "$RS_TMP/after-login.html" \
      --data-urlencode clientDataJSON="$WA_CLIENT_DATA" \
      --data-urlencode authenticatorData="$WA_AUTH_DATA" \
      --data-urlencode signature="$WA_SIGNATURE" \
      --data-urlencode credentialId="$WA_CRED_ID" \
      --data-urlencode userHandle="$WA_USER_HANDLE" \
      -d error=

    if [[ "$KC_LOCATION" == *code=* ]]; then break; fi
    # Keycloak no longer knows this passkey (db reset, user removed it): enroll again, once.
    warn "passkey rejected ($KC_STATUS): $(kc_page_error "$RS_TMP/after-login.html")"
    wa_forget "$dir"
    [[ $attempt -eq 1 ]] || return 1
  done

  [[ "$(query_param "$KC_LOCATION" state)" == "$KC_STATE" ]] || { warn "state mismatch in redirect"; return 1; }
  code="$(query_param "$KC_LOCATION" code)"
  log "passkey 3/3: got authorization code, exchanging it with the PKCE verifier"

  res="$(kurl -X POST "$ISSUER/protocol/openid-connect/token" \
    -d grant_type=authorization_code --data-urlencode client_id="$KC_CLIENT" \
    --data-urlencode code="$code" --data-urlencode redirect_uri="$KC_REDIRECT" \
    --data-urlencode code_verifier="$KC_VERIFIER")"
  token="$(printf '%s' "$res" | jq -r '.access_token // empty' 2>/dev/null)"
  [[ -n "$token" ]] || { warn "code exchange failed: $res"; return 1; }
  printf '%s\n' "$token"
}
