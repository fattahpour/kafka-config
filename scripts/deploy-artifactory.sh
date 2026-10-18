#!/bin/bash
# Uploads the build to a JFrog Artifactory generic repository.
#   ARTIFACTORY_URL    e.g. https://mycompany.jfrog.io/artifactory
#   ARTIFACTORY_REPO   e.g. generic-local
#   ARTIFACTORY_TOKEN  an access token, or ARTIFACTORY_TOKEN_FILE pointing to a file that holds it
#                      (or set ARTIFACTORY_USER and ARTIFACTORY_PASSWORD)
# Files land at <repo>/kafka-config/<version>/; checksums are sent so Artifactory verifies them.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${ARTIFACTORY_URL:?set ARTIFACTORY_URL}" "${ARTIFACTORY_REPO:?set ARTIFACTORY_REPO}"
# reading the token from a file keeps it out of shell history and process lists
if [ -z "${ARTIFACTORY_TOKEN:-}" ] && [ -n "${ARTIFACTORY_TOKEN_FILE:-}" ]; then ARTIFACTORY_TOKEN=$(tr -d '[:space:]' < "$ARTIFACTORY_TOKEN_FILE"); fi
if [ -n "${ARTIFACTORY_TOKEN:-}" ]; then AUTH=(-H "Authorization: Bearer $ARTIFACTORY_TOKEN")
elif [ -n "${ARTIFACTORY_USER:-}" ]; then AUTH=(-u "$ARTIFACTORY_USER:${ARTIFACTORY_PASSWORD:?set ARTIFACTORY_PASSWORD}")
else echo "set ARTIFACTORY_TOKEN, or ARTIFACTORY_USER and ARTIFACTORY_PASSWORD" >&2; exit 1; fi
VERSION=$(node -p "require('./package.json').version")
node scripts/build.js
BASE="${ARTIFACTORY_URL%/}/$ARTIFACTORY_REPO/kafka-config/$VERSION"
for f in "kafka-config-$VERSION.zip" kafka-config.html; do
  echo "uploading $f -> $BASE/$f"
  curl -fsS -X PUT "${AUTH[@]}" \
    -H "X-Checksum-Sha1: $(sha1sum "dist/$f" | cut -d' ' -f1)" \
    -H "X-Checksum-Sha256: $(sha256sum "dist/$f" | cut -d' ' -f1)" \
    -T "dist/$f" "$BASE/$f" -o /dev/null
done
echo "done: $BASE/"
