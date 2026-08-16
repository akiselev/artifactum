#!/usr/bin/env bash
# Real S3/OpenDAL provider exercise against disposable MinIO. Requires Docker.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
for tool in cargo docker curl cmp; do command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 2; }; done
cargo build -p artifactum-cli -p artifactum-provider-s3
export PATH="$ROOT/target/debug:$PATH"
WORK="${ARTIFACTUM_MINIO_TEST_DIR:-$(mktemp -d)}"; KEEP="${ARTIFACTUM_TEST_KEEP:-0}"; NAME="artifactum-minio-$RANDOM-$RANDOM"
cleanup(){ docker rm -f "$NAME" >/dev/null 2>&1 || true; if [[ "$KEEP" != 1 ]]; then rm -rf "$WORK"; else echo "kept MinIO test: $WORK"; fi; }; trap cleanup EXIT
export MINIO_ACCESS_KEY="artifactum-test" MINIO_SECRET_KEY="artifactum-test-secret"
printf 'artifactum minio provider\n' > "$WORK/source.txt"
docker run -d --name "$NAME" -e MINIO_ROOT_USER="$MINIO_ACCESS_KEY" -e MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" -p 127.0.0.1::9000 minio/minio server /data >/dev/null
PORT=$(docker port "$NAME" 9000/tcp | sed -E 's/.*:([0-9]+)$/\1/' | head -1)
for _ in $(seq 1 100); do curl -fsS "http://127.0.0.1:$PORT/minio/health/ready" >/dev/null 2>&1 && break; sleep .1; done
curl -fsS "http://127.0.0.1:$PORT/minio/health/ready" >/dev/null
# Use MinIO's official client in the same network namespace to seed the object.
docker run --rm --network "container:$NAME" -v "$WORK:/fixture:ro" --entrypoint /bin/sh minio/mc -c \
  "mc alias set local http://127.0.0.1:9000 '$MINIO_ACCESS_KEY' '$MINIO_SECRET_KEY' >/dev/null && mc mb --ignore-existing local/artifactum >/dev/null && mc cp /fixture/source.txt local/artifactum/fixture.txt >/dev/null"
PROJ="$WORK/Artifactum.toml"; STORE="$WORK/store"; META="$WORK/meta.sqlite"
A=(artifactum --project "$PROJ" --store "$STORE" --metadata "$META")
"${A[@]}" init --name minio-integration
"${A[@]}" provider add lab s3 \
  --set "endpoint=http://127.0.0.1:$PORT" --set bucket=artifactum --set region=us-east-1 \
  --set 'access_key_id=${MINIO_ACCESS_KEY}' --set 'secret_access_key=${MINIO_SECRET_KEY}' --set enable_virtual_host_style=false
"${A[@]}" add object lab:fixture.txt
ID=$("${A[@]}" fetch object)
"${A[@]}" artifact materialize "$ID" "$WORK/result.txt"
cmp "$WORK/source.txt" "$WORK/result.txt"
"${A[@]}" store verify "$ID"
grep -q 'artifactum_profile.*lab' "$WORK/Artifactum.lock"
if grep -q "$MINIO_SECRET_KEY" "$WORK/Artifactum.lock"; then echo "credential leaked into lockfile" >&2; exit 1; fi
echo "MinIO provider passed: artifact=$ID port=$PORT"
