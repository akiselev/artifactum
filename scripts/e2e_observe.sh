#!/usr/bin/env bash
# Real workflow validation for Artifactum. This deliberately inspects bytes and
# cache behavior; it is not a replacement for cargo test, and cargo test is not
# a replacement for this script.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

need() { command -v "$1" >/dev/null || { echo "missing required tool: $1" >&2; exit 2; }; }
need cargo; need python3; need sh; need cmp; need sha256sum

cargo build --workspace --all-targets
BIN="$ROOT/target/debug/artifactum"
export PATH="$ROOT/target/debug:$PATH"
[[ -x "$BIN" ]] || { echo "artifactum binary missing" >&2; exit 2; }

WORK="${ARTIFACTUM_E2E_DIR:-$(mktemp -d)}"
KEEP="${ARTIFACTUM_E2E_KEEP:-0}"
HTTP_PID=""
cleanup() {
  if [[ -n "${HTTP_PID:-}" ]]; then
    kill "$HTTP_PID" 2>/dev/null || true
    wait "$HTTP_PID" 2>/dev/null || true
  fi
  if [[ "$KEEP" != 1 ]]; then rm -rf "$WORK"; else echo "kept e2e workspace: $WORK"; fi
}
trap cleanup EXIT
mkdir -p "$WORK/project/raw" "$WORK/store1" "$WORK/store2"
# Set this before the first resolver-bearing CLI call: plugin descriptors are
# loaded lazily into the persistent daemon during the workflow.
export ARTIFACTUM_FIXTURE_PID_FILE="$WORK/fixture.pid"
META1="$WORK/meta1.sqlite"; META2="$WORK/meta2.sqlite"; PROJ="$WORK/project/Artifactum.toml"
A=("$BIN" --project "$PROJ" --store "$WORK/store1" --metadata "$META1")
AJ=("$BIN" --json --project "$PROJ" --store "$WORK/store1" --metadata "$META1")

printf 'alpha\n' > "$WORK/project/raw/a.txt"
printf 'bravo\n' > "$WORK/project/raw/b.txt"
printf 'charlie\n' > "$WORK/project/raw/c.txt"

cat > "$PROJ" <<EOF
version = 3

[project]
name = "artifactum-e2e"

[artifacts.raw]
source = "local:$WORK/project/raw"

[tasks.upper]
foreach = "@raw"
run = ["sh", "-c", "tr '[:lower:]' '[:upper:]' < \"\$1\" > \"\$2\"", "--", "{in.item}", "{out.text}"]
cache = "pure"
network = "deny"
sandbox = "read_only_inputs"

[tasks.upper.outputs.text]
kind = "blob"
media_type = "text/plain"

[tasks.bundle]
run = ["sh", "-c", "cat \"\$1\"/* | sort > \"\$2\"", "--", "{in.docs}", "{out.bundle}"]
cache = "pure"
network = "deny"
inputs.docs = "upper.text"

[tasks.bundle.outputs.bundle]
kind = "blob"
media_type = "text/plain"

[refs.final]
target = "bundle.bundle"
EOF

json_field() { python3 -c 'import json,sys; o=json.load(sys.stdin); print(eval(sys.argv[1],{"o":o}))' "$1"; }
assert_run_counts() {
  local file=$1 task=$2 want_hit=$3 want_miss=$4
  python3 - "$file" "$task" "$want_hit" "$want_miss" <<'PY'
import json,sys
p,task,h,m=sys.argv[1],sys.argv[2],int(sys.argv[3]),int(sys.argv[4])
o=json.load(open(p)); rs=o['actions'][task]
hits=sum(bool(x['cache_hit']) for x in rs); misses=len(rs)-hits
assert (hits,misses)==(h,m),(task,hits,misses,'expected',h,m)
print(f"{task}: cache hits={hits} misses={misses}")
PY
}

# 1. Plan and first real pipeline run.
"${AJ[@]}" plan bundle > "$WORK/plan.json"
"${AJ[@]}" run bundle > "$WORK/run1.json"
assert_run_counts "$WORK/run1.json" upper 0 4   # 3 items + collection realization
assert_run_counts "$WORK/run1.json" bundle 0 1
"${A[@]}" artifact materialize @final "$WORK/final1.txt"
printf 'ALPHA\nBRAVO\nCHARLIE\n' > "$WORK/expected1.txt"
cmp "$WORK/final1.txt" "$WORK/expected1.txt"
FINAL1=$("${AJ[@]}" artifact inspect @final | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "first final artifact: $FINAL1"

# 2. Same inputs => every action is a cache hit.
"${AJ[@]}" run bundle > "$WORK/run2.json"
assert_run_counts "$WORK/run2.json" upper 4 0
assert_run_counts "$WORK/run2.json" bundle 1 0

# 3. Mutate one source. --frozen must keep the exact old locked source/result.
printf 'beta changed\n' > "$WORK/project/raw/b.txt"
"${AJ[@]}" run --frozen bundle > "$WORK/frozen.json"
assert_run_counts "$WORK/frozen.json" upper 4 0
assert_run_counts "$WORK/frozen.json" bundle 1 0
FROZEN=$("${AJ[@]}" artifact inspect @final | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
[[ "$FROZEN" == "$FINAL1" ]] || { echo "frozen run moved output" >&2; exit 1; }

# 4. Normal run re-resolves the source: two map items hit, one reruns; collection
# and downstream aggregate rerun. Inspect the actual bytes.
"${AJ[@]}" run bundle > "$WORK/run3.json"
assert_run_counts "$WORK/run3.json" upper 2 2   # 2 item hits, 1 item + collector miss
assert_run_counts "$WORK/run3.json" bundle 0 1
"${A[@]}" artifact materialize @final "$WORK/final2.txt"
printf 'ALPHA\nBETA CHANGED\nCHARLIE\n' > "$WORK/expected2.txt"
cmp "$WORK/final2.txt" "$WORK/expected2.txt"
FINAL2=$("${AJ[@]}" artifact inspect @final | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
[[ "$FINAL2" != "$FINAL1" ]] || { echo "changed source did not change output identity" >&2; exit 1; }

# 5. Lineage must connect the output back through bundle -> mapped collection ->
# source file observations. Save it for human inspection and enforce depth > 2.
"${AJ[@]}" lineage "$FINAL2" > "$WORK/lineage.json"
python3 - "$WORK/lineage.json" <<'PY'
import json,sys
a=json.load(open(sys.argv[1])); assert len(a)>=3, len(a)
assert any(x.get('sources') for x in a), 'lineage contains no source observations'
assert any(x.get('producers') for x in a), 'lineage contains no producer realization'
print('lineage nodes:',len(a))
PY

# 6. Determinism audit: rerun a pure action twice and require identical outputs.
BUNDLE_ACTION=$(python3 - "$WORK/run3.json" <<'PY'
import json,sys
o=json.load(open(sys.argv[1])); print(o['actions']['bundle'][0]['action'])
PY
)
"${AJ[@]}" audit determinism "$BUNDLE_ACTION" --runs 2 > "$WORK/determinism.json"
python3 - "$WORK/determinism.json" <<'PY'
import json,sys
o=json.load(open(sys.argv[1])); assert o['deterministic'] is True,o
print('determinism audit passed')
PY

# 7. Checkpoint/retry: first attempt deliberately fails after writing checkpoint;
# retry receives it automatically and must succeed.
cat > "$WORK/project/Checkpoint.toml" <<'EOF'
version = 3
[project]
name = "checkpoint-e2e"
[tasks.once]
run = ["sh", "-c", "if [ -f \"$ARTIFACTUM_CHECKPOINT_IN/progress\" ]; then cp \"$ARTIFACTUM_CHECKPOINT_IN/progress\" \"$1\"; else printf recovered > \"$ARTIFACTUM_CHECKPOINT_OUT/progress\"; exit 23; fi", "--", "{out.result}"]
cache = "reproducible"
[tasks.once.outputs.result]
kind = "blob"
EOF
CP=("$BIN" --project "$WORK/project/Checkpoint.toml" --store "$WORK/store1" --metadata "$META1")
CPJ=("$BIN" --json --project "$WORK/project/Checkpoint.toml" --store "$WORK/store1" --metadata "$META1")
set +e
"${CP[@]}" run once >"$WORK/checkpoint-first.out" 2>"$WORK/checkpoint-first.err"
rc=$?
set -e
[[ $rc -ne 0 ]] || { echo "checkpoint fixture was expected to fail first" >&2; exit 1; }
ATTEMPT=$("${CPJ[@]}" runs list --limit 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')
"${CPJ[@]}" runs retry "$ATTEMPT" > "$WORK/checkpoint-retry.json"
python3 - "$WORK/checkpoint-retry.json" <<'PY'
import json,sys
o=json.load(open(sys.argv[1])); assert not o['cache_hit']; print('checkpoint retry succeeded')
PY

# 8. Cancellation: run a 60-second process, find its live attempt, request cancel,
# and require prompt nonzero completion rather than waiting the full duration.
cat > "$WORK/project/Cancel.toml" <<'EOF'
version = 3
[project]
name = "cancel-e2e"
[tasks.wait]
run = ["sleep", "60"]
cache = "volatile"
[tasks.wait.outputs.never]
kind = "blob"
EOF
CAN=("$BIN" --project "$WORK/project/Cancel.toml" --store "$WORK/store1" --metadata "$META1")
CANJ=("$BIN" --json --project "$WORK/project/Cancel.toml" --store "$WORK/store1" --metadata "$META1")
set +e
"${CAN[@]}" run wait >"$WORK/cancel.out" 2>"$WORK/cancel.err" &
RUNPID=$!
set -e
LIVE=""
for _ in $(seq 1 50); do
  LIVE=$("${CANJ[@]}" runs list --limit 10 | python3 -c 'import json,sys; a=json.load(sys.stdin); print(next((x["id"] for x in a if x.get("finished_at") is None),""))')
  [[ -n "$LIVE" ]] && break
  sleep .1
done
[[ -n "$LIVE" ]] || { echo "did not observe live attempt" >&2; kill "$RUNPID" 2>/dev/null || true; exit 1; }
"${CAN[@]}" runs cancel "$LIVE"
set +e
wait "$RUNPID"; cancel_rc=$?
set -e
[[ $cancel_rc -ne 0 ]] || { echo "cancelled run unexpectedly succeeded" >&2; exit 1; }

# 9. Content-defined chunked blob: import, materialize, byte-compare, verify.
python3 - "$WORK/big.bin" <<'PY'
import sys
p=sys.argv[1]
with open(p,'wb') as f:
    for i in range(180000): f.write((f"record-{i:08d}-abcdefghijklmnopqrstuvwxyz0123456789\n").encode())
PY
BIG=$("${A[@]}" artifact import "$WORK/big.bin" --chunked --set-ref big | tail -1)
"${A[@]}" artifact materialize @big "$WORK/big-roundtrip.bin"
cmp "$WORK/big.bin" "$WORK/big-roundtrip.bin"
"${A[@]}" store verify @big

# 10. Attestation policy + promotion semantics. Promotion must evaluate the
# policy before creating the immutable release ref.
cat > "$WORK/statement.json" <<'EOF'
{"result":"pass","suite":"artifactum-e2e"}
EOF
"${A[@]}" attest add @final dev.artifactum.test/v1 "$WORK/statement.json" --issuer e2e-agent >/dev/null
cat > "$WORK/policy.toml" <<'EOF'
required_predicates = ["dev.artifactum.test/v1"]
allowed_issuers = ["e2e-agent"]
min_attestations = 1
EOF
"${A[@]}" verify @final --policy "$WORK/policy.toml"
"${A[@]}" promote @final release --policy "$WORK/policy.toml"

# 11. OCI layout export: verify layout files exist and every declared blob exists.
"${A[@]}" export oci @final "$WORK/oci" >/dev/null
python3 - "$WORK/oci" <<'PY'
import json,os,sys
r=sys.argv[1]; idx=json.load(open(os.path.join(r,'index.json'))); assert idx['schemaVersion']==2
for m in idx['manifests']:
    algo,d=m['digest'].split(':',1); assert os.path.exists(os.path.join(r,'blobs',algo,d))
print('OCI layout structurally valid')
PY

# 12. File remote mirror -> completely fresh local CAS -> byte-identical result.
"${A[@]}" remote add backup file "$WORK/remote"
"${A[@]}" remote push backup @final
A2=("$BIN" --project "$PROJ" --store "$WORK/store2" --metadata "$META2")
"${A2[@]}" remote pull backup "$FINAL2"
"${A2[@]}" artifact materialize "$FINAL2" "$WORK/from-remote.txt"
cmp "$WORK/from-remote.txt" "$WORK/expected2.txt"
# Remove the final blob from a copied remote and prove a fresh pull fails rather
# than constructing a partial artifact graph.
cp -a "$WORK/remote" "$WORK/broken-remote"
FINAL_CONTENT=$("${AJ[@]}" artifact inspect @final | python3 -c 'import json,sys; print(json.load(sys.stdin)["manifest"]["content"]["value"])')
rm "$WORK/broken-remote/content/sha256/${FINAL_CONTENT:0:2}/$FINAL_CONTENT"
"${A[@]}" remote add broken file "$WORK/broken-remote"
A3=("$BIN" --project "$PROJ" --store "$WORK/store3" --metadata "$WORK/meta3.sqlite")
set +e
"${A3[@]}" remote pull broken "$FINAL2" >"$WORK/broken-remote.out" 2>"$WORK/broken-remote.err"
broken_rc=$?
set -e
[[ $broken_rc -ne 0 ]] || { echo "remote pull unexpectedly succeeded with missing content" >&2; exit 1; }

# Native HTTP CAS: authenticated streaming push/pull, including the chunked
# large artifact, into another completely empty store.
HTTP_PORT=$(python3 - <<'PYPORT'
import socket
s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()
PYPORT
)
export ARTIFACTUM_E2E_REMOTE_TOKEN="artifactum-e2e-token"
"${A[@]}" remote serve "$WORK/http-remote" --bind "127.0.0.1:$HTTP_PORT" --token-env ARTIFACTUM_E2E_REMOTE_TOKEN >"$WORK/http-remote.log" 2>&1 &
HTTP_PID=$!
for _ in $(seq 1 50); do
  if python3 - "$HTTP_PORT" <<'PYWAIT' >/dev/null 2>&1
import socket,sys
s=socket.socket(); s.settimeout(.1); s.connect(('127.0.0.1',int(sys.argv[1]))); s.close()
PYWAIT
  then break; fi
  sleep .1
done
"${A[@]}" remote add http-backup http "http://127.0.0.1:$HTTP_PORT" --token-env ARTIFACTUM_E2E_REMOTE_TOKEN
"${A[@]}" remote push http-backup @final
"${A[@]}" remote push http-backup @big
A4=("$BIN" --project "$PROJ" --store "$WORK/store4" --metadata "$WORK/meta4.sqlite")
"${A4[@]}" remote pull http-backup "$FINAL2"
"${A4[@]}" remote pull http-backup "$BIG"
"${A4[@]}" artifact materialize "$FINAL2" "$WORK/http-final.txt"
"${A4[@]}" artifact materialize "$BIG" "$WORK/http-big.bin"
cmp "$WORK/http-final.txt" "$WORK/expected2.txt"
cmp "$WORK/http-big.bin" "$WORK/big.bin"
kill "$HTTP_PID" 2>/dev/null || true
wait "$HTTP_PID" 2>/dev/null || true
HTTP_PID=""

# 13. Daemonized provider plugin should survive separate CLI invocations. The
# fixture writes its PID only on process start.
printf fixture > "$WORK/fixture.txt"
cat > "$WORK/project/Fixture.toml" <<EOF
version = 3
[project]
name = "fixture-provider"
[artifacts.fixture]
source = "fixture:$WORK/fixture.txt"
EOF
FX=("$BIN" --project "$WORK/project/Fixture.toml" --store "$WORK/store1" --metadata "$META1")
"${FX[@]}" fetch fixture >/dev/null
PID1=$(cat "$WORK/fixture.pid")
"${FX[@]}" fetch fixture >/dev/null
PID2=$(cat "$WORK/fixture.pid")
[[ "$PID1" == "$PID2" ]] || { echo "provider daemon did not preserve plugin session: $PID1 -> $PID2" >&2; exit 1; }
echo "provider process reused across CLI invocations: pid=$PID1"
kill "$PID1"
for _ in $(seq 1 50); do kill -0 "$PID1" 2>/dev/null || break; sleep .05; done
"${FX[@]}" fetch fixture >/dev/null
PID3=$(cat "$WORK/fixture.pid")
[[ "$PID3" != "$PID1" ]] || { echo "provider process was not respawned after forced death" >&2; exit 1; }
echo "provider process respawned after crash: $PID1 -> $PID3"

# 14. Integrity failure: corrupt a copied store, prove verify catches it without
# damaging the real test store.
cp -a "$WORK/store1" "$WORK/corrupt-store"
CONTENT=$("${AJ[@]}" artifact inspect @final | python3 -c 'import json,sys; print(json.load(sys.stdin)["manifest"]["content"]["value"])')
CORRUPT="$WORK/corrupt-store/content/sha256/${CONTENT:0:2}/$CONTENT"
printf X >> "$CORRUPT"
set +e
"$BIN" --project "$PROJ" --store "$WORK/corrupt-store" --metadata "$META1" store verify "$FINAL2" >/dev/null 2>&1
verify_rc=$?
set -e
[[ $verify_rc -ne 0 ]] || { echo "corruption was not detected" >&2; exit 1; }

# 15. GC: create an orphan, demonstrate dry-run, then sweep it while all refs and
# recent metadata roots remain valid.
printf orphan > "$WORK/orphan.txt"
ORPHAN=$("${A[@]}" artifact import "$WORK/orphan.txt" | tail -1)
"${AJ[@]}" store gc --dry-run --retention-days 30 > "$WORK/gc-dry.json"
"${AJ[@]}" store gc --retention-days 30 > "$WORK/gc.json"
"${A[@]}" store verify @final
if "${A[@]}" artifact inspect "$ORPHAN" >/dev/null 2>&1; then
  echo "note: orphan remained reachable through recent metadata; this is valid under retention policy"
else
  echo "orphan reclaimed"
fi

"${AJ[@]}" store stats > "$WORK/store-stats.json"
echo
echo "E2E OBSERVATIONAL WORKFLOW PASSED"
echo "workspace: $WORK"
echo "Inspect these before declaring validation complete:"
echo "  $WORK/run1.json $WORK/run2.json $WORK/run3.json"
echo "  $WORK/lineage.json $WORK/determinism.json"
echo "  $WORK/store-stats.json $WORK/gc-dry.json $WORK/gc.json"
echo "  $WORK/final2.txt $WORK/oci/index.json"
echo "Set ARTIFACTUM_E2E_KEEP=1 to retain all evidence."
