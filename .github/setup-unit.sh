#!/usr/bin/env bash
set -euo pipefail

# --- Local configuration ---
CERT_DIR="certs"
CERT_FILE="${CERT_DIR}/cert.pem"
KEY_FILE="${CERT_DIR}/key.pem"
APP_YML="src/test/resources/application-test.yml"
GITEA_HOST="localhost"
GITEA_PORT="3443"
GITEA_BASE="https://${GITEA_HOST}:${GITEA_PORT}"
GITEA_ADMIN_USER="gitea_admin"
GITEA_ADMIN_PASS="password123"

# Docker compose command (compatibility between compose v2 and v1)
DOCKER_COMPOSE="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  if command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
  fi
fi

# --- 1) Generate self-signed certificate with SAN (localhost + 127.0.0.1) if not already present ---
mkdir -p "${CERT_DIR}"
if [[ ! -f "${CERT_FILE}" ]]; then
  echo "[setup] Generating self-signed cert with SAN (localhost, 127.0.0.1)"
  openssl req -x509 -newkey rsa:2048 \
    -keyout "${KEY_FILE}" -out "${CERT_FILE}" \
    -days 365 -nodes -subj "/CN=localhost" \
    -addext "subjectAltName = DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1

  chmod 644 "${CERT_FILE}" "${KEY_FILE}" || true
  # On some CI runners, chown may require sudo; ignore errors if it fails
  sudo chown 1000:1000 "${CERT_FILE}" "${KEY_FILE}" 2>/dev/null || true
else
  echo "[setup] Using existing cert: ${CERT_FILE}"
fi

echo "[setup] Certificate SAN check:"
openssl x509 -in "${CERT_FILE}" -noout -text | grep -A1 "Subject Alternative Name" || true
echo "[setup] Certificate fingerprint:"
openssl x509 -noout -fingerprint -sha256 -in "${CERT_FILE}" || true

# --- 2) Start environment (Gitea exposed on https://localhost:3443) ---
echo "[setup] Starting containers"
${DOCKER_COMPOSE} up -d

# --- 3) Wait for Gitea to respond over HTTPS (phase 1: permissive TLS while it starts) ---
echo "[setup] Waiting for Gitea (initial, permissive TLS)"
for i in {1..120}; do
  if curl -sf -k "${GITEA_BASE}/api/v1/version" >/dev/null; then
    echo "[setup] Gitea responded (in permissive mode) after ${i} attempts"
    break
  fi
  if [[ $((i % 20)) -eq 0 ]]; then
    docker ps --filter "name=gitea" --format "table {{.Names}}\t{{.Status}}"
  fi
  sleep 3
done

if ! curl -sf -k "${GITEA_BASE}/api/v1/version" >/dev/null; then
  echo "[setup] ERROR: Gitea not responding after ~6 minutes (permissive mode)."
  docker ps --filter "name=gitea" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}"
  docker logs $(docker ps --filter "name=gitea" -q | head -n1) --tail 100 || true
  exit 1
fi

# --- 4) Detect app.ini path inside container (supports both /data/gitea/conf and /etc/gitea) ---
CONTAINER=$(docker ps --filter "name=gitea" -q | head -n1)
if [[ -z "${CONTAINER}" ]]; then
  echo "[setup] ERROR: No Gitea container found!"
  docker ps
  exit 1
fi

# Determine the most likely app.ini location inside the container
APP_INI=$(docker exec "${CONTAINER}" sh -c '
  if [ -f /data/gitea/conf/app.ini ]; then
    echo /data/gitea/conf/app.ini
  elif [ -d /data/gitea/conf ]; then
    echo /data/gitea/conf/app.ini
  elif [ -f /etc/gitea/app.ini ]; then
    echo /etc/gitea/app.ini
  elif [ -d /etc/gitea ]; then
    echo /etc/gitea/app.ini
  else
    echo /data/gitea/conf/app.ini
  fi
' | tr -d '\r')

echo "[setup] Detected app.ini path in container: ${APP_INI}"

# Ensure parent directory exists and file is present, as root (no login shell)
docker exec -u root "${CONTAINER}" sh -c "
  mkdir -p \"\$(dirname \"${APP_INI}\")\"
  touch \"${APP_INI}\"
  chown -R git:git \"\$(dirname \"${APP_INI}\")\" 2>/dev/null || true
"

# --- 5) Copy cert/key into container under /data/certs and set permissions ---
echo "[setup] Ensuring Gitea uses our certificate"
docker exec -u root "${CONTAINER}" sh -c "mkdir -p /data/certs"
docker cp "${CERT_FILE}" "${CONTAINER}":/data/certs/cert.pem
docker cp "${KEY_FILE}"  "${CONTAINER}":/data/certs/key.pem
docker exec -u root "${CONTAINER}" sh -c "chown -R git:git /data/certs"

# --- 6) Configure app.ini [server] CERT_FILE/KEY_FILE regardless of its actual location ---
docker exec "${CONTAINER}" sh -c "
  set -e
  APP_INI='${APP_INI}'
  # Ensure [server] section exists
  grep -q '^\[server\]' \"\$APP_INI\" || echo '[server]' >> \"\$APP_INI\"
  # Set CERT_FILE and KEY_FILE (create or replace)
  if grep -q '^CERT_FILE' \"\$APP_INI\"; then
    sed -i 's#^CERT_FILE *=.*#CERT_FILE = /data/certs/cert.pem#' \"\$APP_INI\"
  else
    echo 'CERT_FILE = /data/certs/cert.pem' >> \"\$APP_INI\"
  fi
  if grep -q '^KEY_FILE' \"\$APP_INI\"; then
    sed -i 's#^KEY_FILE *=.*#KEY_FILE = /data/certs/key.pem#' \"\$APP_INI\"
  else
    echo 'KEY_FILE = /data/certs/key.pem' >> \"\$APP_INI\"
  fi
"

echo "[setup] Restarting Gitea to apply certificate"
docker restart "${CONTAINER}" >/dev/null

# --- 7) Wait for Gitea to be ready in STRICT TLS mode with our CA ---
echo "[setup] Waiting for Gitea (strict TLS with our CA)"
for i in {1..120}; do
  if curl -sf --cacert "${CERT_FILE}" "${GITEA_BASE}/api/v1/version" >/dev/null; then
    echo "[setup] Gitea (strict TLS) is ready after ${i} attempts"
    break
  fi
  sleep 3
done

if ! curl -sf --cacert "${CERT_FILE}" "${GITEA_BASE}/api/v1/version" >/dev/null; then
  echo "[setup] ERROR: Gitea not responding in strict TLS after ~6 minutes."
  docker logs "${CONTAINER}" --tail 100 || true
  exit 1
fi

# --- 7.1) CI DEBUG: verify PEM visibility, perms, and server certificate match ---
echo "[debug] ===== CI TLS DEBUG START ====="
echo "[debug] PWD: $(pwd)"
echo "[debug] Listing cert directory:"
ls -l "${CERT_DIR}" || true
echo "[debug] PEM file info:"
ls -l "${CERT_FILE}" || echo "[debug] Missing PEM at ${CERT_FILE}"
file "${CERT_FILE}" || true
echo "[debug] PEM fingerprint (sha256):"
openssl x509 -noout -fingerprint -sha256 -in "${CERT_FILE}" || true
echo "[debug] PEM subject / issuer:"
openssl x509 -noout -subject -issuer -in "${CERT_FILE}" || true
echo "[debug] PEM SAN (should include DNS:localhost and/or IP:127.0.0.1):"
openssl x509 -in "${CERT_FILE}" -noout -text | sed -n '/Subject Alternative Name/,+1p' || true

echo "[debug] Server-presented certificate fingerprint (sha256):"
SERVER_FP=$(openssl s_client -connect ${GITEA_HOST}:${GITEA_PORT} -servername ${GITEA_HOST} </dev/null 2>/dev/null | openssl x509 -noout -fingerprint -sha256 | tr -d '\r')
echo "[debug] ${SERVER_FP:-<empty>}"

echo "[debug] Strict curl with PEM (verbose):"
set +e
curl -v --cacert "${CERT_FILE}" "${GITEA_BASE}/api/v1/version"
CURL_RC=$?
set -e
echo "[debug] curl exit code: ${CURL_RC}"
echo "[debug] ===== CI TLS DEBUG END ====="

# --- 8) Create admin user (idempotent) ---
echo "[setup] Ensuring admin user exists"
docker exec -u git "${CONTAINER}" gitea admin user create \
  --username "${GITEA_ADMIN_USER}" --password "${GITEA_ADMIN_PASS}" \
  --email "${GITEA_ADMIN_USER}@example.com" --admin 2>/dev/null || true

sleep 3

# --- 9) Generate PAT (CLI first, then API fallback) ---
echo "[setup] Generating token (PAT) via CLI (with --raw if available)"
TOKEN_NAME="ci-$(date +%s)"
set +e
GITEA_PAT=$(docker exec -u git "${CONTAINER}" sh -c \
  "gitea admin user generate-access-token --username '${GITEA_ADMIN_USER}' --scopes 'all' --token-name '${TOKEN_NAME}' --raw 2>/dev/null" \
  | tr -d '\r')
set -e

if [[ -z "${GITEA_PAT}" ]]; then
  echo "[setup] CLI --raw failed or not supported; trying legacy CLI parsing"
  set +e
  GITEA_PAT=$(docker exec -u git "${CONTAINER}" sh -c \
    "gitea admin user generate-access-token --username '${GITEA_ADMIN_USER}' --scopes 'all' 2>/dev/null" \
    | grep -oE '[a-f0-9]{40}' | head -n1 | tr -d '\r')
  set -e
fi

if [[ -z "${GITEA_PAT}" ]]; then
  echo "[setup] CLI generation failed; falling back to API"
  API_RESP=$(curl -sS --cacert "${CERT_FILE}" -u "${GITEA_ADMIN_USER}:${GITEA_ADMIN_PASS}" \
    -H "Content-Type: application/json" \
    -X POST "${GITEA_BASE}/api/v1/users/${GITEA_ADMIN_USER}/tokens" \
    -d "{\"name\":\"${TOKEN_NAME}\",\"scopes\":[\"all\"]}" || true)
  GITEA_PAT=$(echo "${API_RESP}" | sed -n 's/.*"sha1"[[:space:]]*:[[:space:]]*"\([^"]\{40\}\)".*/\1/p' | head -n1)
fi

if [[ -z "${GITEA_PAT}" ]]; then
  echo "[setup] ERROR: Failed to generate PAT (CLI and API fallback)"
  echo "[setup] Debug API response: ${API_RESP:-<empty>}"
  exit 1
fi

echo "[setup] Generated PAT: ${GITEA_PAT}"

# Validate PAT with strict TLS
curl -sf --cacert "${CERT_FILE}" -H "Authorization: token ${GITEA_PAT}" \
  "${GITEA_BASE}/api/v1/user" >/dev/null || {
  echo "[setup] ERROR: Token validation failed (strict TLS)"; exit 1;
}

# --- 10) Create repository and README if not existing ---
echo "[setup] Creating repository (if needed)"
curl -sf --cacert "${CERT_FILE}" -X POST "${GITEA_BASE}/api/v1/user/repos" \
  -H "Authorization: token ${GITEA_PAT}" -H "Content-Type: application/json" \
  -d '{"name":"kestra-test"}' >/dev/null || true

README_B64=$(echo -n "# Kestra Test Repository" | base64 | tr -d '\n')
curl -sf --cacert "${CERT_FILE}" -X POST \
  "${GITEA_BASE}/api/v1/repos/${GITEA_ADMIN_USER}/kestra-test/contents/README.md" \
  -H "Authorization: token ${GITEA_PAT}" -H "Content-Type: application/json" \
  -d "{\"content\":\"${README_B64}\",\"message\":\"Initial commit\"}" >/dev/null || true

# --- 11) Write test configuration file for JUnit classes ---
echo "[setup] Writing ${APP_YML}"
mkdir -p "$(dirname "${APP_YML}")"
cat > "${APP_YML}" <<EOF
kestra:
  git:
    pat: \${GH_PERSONAL_TOKEN:}
  gitea:
    pat: ${GITEA_PAT}
    repository-url: ${GITEA_BASE}/${GITEA_ADMIN_USER}/kestra-test.git
    user:
      name: ${GITEA_ADMIN_USER}
      email: admin@gitea.local
    # Absolute path to the PEM (served by Gitea)
    ca-pem-path: "$(pwd)/${CERT_FILE}"
  repository:
    type: memory
  queue:
    type: memory
  storage:
    type: local
    local:
      base-path: /tmp/unittest
EOF

echo "[setup] Summary:"
echo "  - Repository URL: ${GITEA_BASE}/${GITEA_ADMIN_USER}/kestra-test.git"
echo "  - PAT: ${GITEA_PAT}"
echo "  - CA PEM (strict): $(pwd)/${CERT_FILE}"
ls -l "${CERT_DIR}" || true
