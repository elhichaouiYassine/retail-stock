# shellcheck shell=bash
# shellcheck disable=SC2034  # variables here are read by the scripts that source this file
# A tiny software passkey (FIDO2 / WebAuthn authenticator) in bash + openssl.
#
# It does what a phone or laptop does when you tap "use passkey":
#   register: make a P-256 key pair, hand Keycloak the public key (attestation "none")
#   login:    sign Keycloak's challenge with the private key (an "assertion")
#
# The key lives in .e2e/passkeys/<keycloak-host>/<user>/ so later runs log in with the same passkey.
# Test use only: the private key sits unencrypted on disk.
#
# Byte layouts (WebAuthn spec, section 6.1):
#   authenticatorData = sha256(rpId) | flags(1) | signCount(4, big endian) [| attestedCredentialData]
#   attestedCredentialData = aaguid(16) | credIdLen(2) | credId | COSE public key
#   flags: 0x01 user present, 0x04 user verified, 0x40 attested data included

# ---------------------------------------------------------------- byte helpers

hex_of_file() { od -An -v -tx1 "$1" | tr -d ' \n'; }
hex_of_stdin() { od -An -v -tx1 | tr -d ' \n'; }

# Writes hex as raw bytes. printf '%b' with \xNN handles NUL bytes when writing to a file.
hex_to_file() { printf '%b' "$(printf '%s' "$1" | sed 's/../\\x&/g')" >"$2"; }

hex_b64url() {
  local f
  f="$(mktemp "${RS_TMP:-/tmp}/wa.XXXXXX")"
  hex_to_file "$1" "$f"
  b64url_encode <"$f"
  rm -f "$f"
}

sha256_hex() { openssl dgst -sha256 -binary | hex_of_stdin; }

# CBOR header for a byte string of <n> bytes (major type 2).
cbor_bytes_header() {
  local n=$1
  if ((n < 24)); then printf '%02x' $((0x40 + n))
  elif ((n < 256)); then printf '58%02x' "$n"
  else printf '59%04x' "$n"
  fi
}

# ---------------------------------------------------------------- state

# Folder for one user's passkey on one Keycloak (local and minikube keys never mix).
wa_dir() {
  local host
  host="$(printf '%s' "$KEYCLOAK_URL" | sed -E 's#^[a-z]+://##; s#[^A-Za-z0-9.-]#_#g')"
  echo "$RS_STATE_DIR/passkeys/$host/$1"
}

wa_has_credential() { [[ -f "$1/key.pem" && -f "$1/credential_id" && -f "$1/user_handle" ]]; }

wa_forget() { rm -rf "$1"; }

# ---------------------------------------------------------------- register

# wa_register <dir> <challenge> <rpId> <origin> <userHandle>
# Sets WA_CLIENT_DATA, WA_ATTESTATION, WA_CRED_ID (all base64url) for the registration form.
wa_register() {
  local dir=$1 challenge=$2 rp_id=$3 origin=$4 user_handle=$5
  local pub_hex x y cred_hex rp_hash cose auth_data att_obj
  mkdir -p "$dir"
  chmod 700 "$dir"

  # 1. New P-256 key pair (COSE alg -7 = ES256).
  openssl ecparam -name prime256v1 -genkey -noout -out "$dir/key.pem" 2>/dev/null
  chmod 600 "$dir/key.pem"
  # The last 65 bytes of the DER public key are the raw point: 04 | X(32) | Y(32).
  pub_hex="$(openssl ec -in "$dir/key.pem" -pubout -outform DER 2>/dev/null | tail -c 65 | hex_of_stdin)"
  x="${pub_hex:2:64}"
  y="${pub_hex:66:64}"

  # 2. Random 32-byte credential id.
  cred_hex="$(openssl rand 32 | hex_of_stdin)"

  # 3. COSE_Key map {1:2 (EC2), 3:-7 (ES256), -1:1 (P-256), -2:X, -3:Y}.
  cose="a5""0102""0326""2001""215820${x}""225820${y}"

  # 4. authenticatorData with flags 0x45 (present + verified + attested data), counter 0, zero AAGUID.
  rp_hash="$(printf '%s' "$rp_id" | sha256_hex)"
  auth_data="${rp_hash}45""00000000""00000000000000000000000000000000""0020${cred_hex}${cose}"

  # 5. attestationObject = CBOR {"fmt":"none", "attStmt":{}, "authData":<bytes>}.
  #   a3 = map(3), 63/64/67/68 = text strings of length 3/4/7/8, a0 = empty map
  att_obj="a3""63666d74""646e6f6e65""6761747453746d74""a0""686175746844617461"
  att_obj="${att_obj}$(cbor_bytes_header $((${#auth_data} / 2)))${auth_data}"

  # 6. clientDataJSON: what a browser would send. Keycloak checks type, challenge and origin.
  WA_CLIENT_DATA="$(printf '{"type":"webauthn.create","challenge":"%s","origin":"%s","crossOrigin":false}' \
    "$challenge" "$origin" | b64url_encode)"
  WA_ATTESTATION="$(hex_b64url "$att_obj")"
  WA_CRED_ID="$(hex_b64url "$cred_hex")"

  printf '%s' "$WA_CRED_ID" >"$dir/credential_id"
  printf '%s' "$user_handle" >"$dir/user_handle"
  echo 0 >"$dir/sign_count"
}

# ---------------------------------------------------------------- login

# wa_assert <dir> <challenge> <rpId> <origin>
# Sets WA_CLIENT_DATA, WA_AUTH_DATA, WA_SIGNATURE, WA_CRED_ID, WA_USER_HANDLE for the login form.
wa_assert() {
  local dir=$1 challenge=$2 rp_id=$3 origin=$4
  local count rp_hash auth_hex cdj cdj_hash signed
  count=$(($(cat "$dir/sign_count" 2>/dev/null || echo 0) + 1))
  echo "$count" >"$dir/sign_count"

  # authenticatorData: flags 0x05 (present + verified), counter goes up on every use.
  rp_hash="$(printf '%s' "$rp_id" | sha256_hex)"
  auth_hex="${rp_hash}05$(printf '%08x' "$count")"

  cdj="$(printf '{"type":"webauthn.get","challenge":"%s","origin":"%s","crossOrigin":false}' "$challenge" "$origin")"
  cdj_hash="$(printf '%s' "$cdj" | sha256_hex)"

  # Signature = ECDSA-SHA256 over authenticatorData | sha256(clientDataJSON), DER encoded.
  signed="$(mktemp "${RS_TMP:-/tmp}/wa.XXXXXX")"
  hex_to_file "${auth_hex}${cdj_hash}" "$signed"
  WA_SIGNATURE="$(openssl dgst -sha256 -sign "$dir/key.pem" "$signed" | b64url_encode)"
  rm -f "$signed"

  WA_CLIENT_DATA="$(printf '%s' "$cdj" | b64url_encode)"
  WA_AUTH_DATA="$(hex_b64url "$auth_hex")"
  WA_CRED_ID="$(cat "$dir/credential_id")"
  WA_USER_HANDLE="$(cat "$dir/user_handle")"
}
