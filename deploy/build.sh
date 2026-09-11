#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=$(python -c 'import tomllib;print(tomllib.load(open("pyproject.toml","rb"))["project"]["version"])')
export OEAMDB_WEB_VERSION="$VERSION"
export OEAMDB_WEB_IMAGE_TAG="${VERSION//+/_}"
export OEAMDB_WEB_GIT_SHA="$(git rev-parse --short HEAD)$(git diff --quiet || echo -dirty)"

cd deploy
bash gen_env.sh

source .env

CERT_FILE=$OEAMDB_TLS_CERT
KEY_FILE=$OEAMDB_TLS_KEY

if [[ -e $CERT_FILE || -e $KEY_FILE ]]; then
    echo "Certificate files already present in $CERT_FILE, skipping."
    if [[ ! -e $CERT_FILE || ! -e $KEY_FILE ]]; then
        echo "Warning: only one of the two files exists, Caddy will fail to start." >&2
    fi
else
    mkdir -p $(dirname "$CERT_FILE")
    mkdir -p $(dirname "$KEY_FILE")
    openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 365 \
      -keyout "$KEY_FILE" -out "$CERT_FILE" \
      -subj "/CN=${OEAMDB_DOMAIN}" \
      -addext "subjectAltName=DNS:${OEAMDB_DOMAIN},IP:127.0.0.1"
    echo "Generated self-signed certificate for $OEAMDB_DOMAIN: $CERT_FILE and $KEY_FILE"
fi

exec docker compose up --build "$@"
