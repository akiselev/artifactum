#!/usr/bin/env bash
# Real Git + Git LFS provider exercise against a disposable local bare remote.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
for tool in cargo git cmp python3; do command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 2; }; done
git lfs version >/dev/null 2>&1 || { echo "git-lfs is required" >&2; exit 2; }
cargo build -p artifactum-cli -p artifactum-provider-git
export PATH="$ROOT/target/debug:$PATH"
WORK="${ARTIFACTUM_GIT_LFS_TEST_DIR:-$(mktemp -d)}"; KEEP="${ARTIFACTUM_TEST_KEEP:-0}"; trap 'if [[ "$KEEP" != 1 ]]; then rm -rf "$WORK"; else echo "kept Git LFS test: $WORK"; fi' EXIT
mkdir "$WORK/origin"; git -C "$WORK/origin" init -b main >/dev/null; git -C "$WORK/origin" config user.email artifactum@example.invalid; git -C "$WORK/origin" config user.name Artifactum
git -C "$WORK/origin" lfs install --local >/dev/null; git -C "$WORK/origin" lfs track '*.bin' >/dev/null
python3 - "$WORK/origin/payload.bin" <<'PY'
import sys
open(sys.argv[1],'wb').write((b'old-lfs-payload-'*131072)[:1500000])
PY
cp "$WORK/origin/payload.bin" "$WORK/expected-old.bin"
git -C "$WORK/origin" add .gitattributes payload.bin; git -C "$WORK/origin" commit -m initial >/dev/null
git init --bare "$WORK/remote.git" >/dev/null; git -C "$WORK/origin" remote add fixture "file://$WORK/remote.git"; git -C "$WORK/origin" push fixture main >/dev/null; git -C "$WORK/origin" lfs push fixture --all >/dev/null
PROJ="$WORK/Artifactum.toml"; STORE="$WORK/store"; META="$WORK/meta.sqlite"; A=(artifactum --project "$PROJ" --store "$STORE" --metadata "$META")
"${A[@]}" init --name git-lfs-integration; "${A[@]}" add payload "git:file://$WORK/remote.git#payload.bin" --revision main
OLD=$("${A[@]}" fetch payload); "${A[@]}" artifact materialize "$OLD" "$WORK/old.bin"; cmp "$WORK/expected-old.bin" "$WORK/old.bin"
# Move the mutable branch to genuinely different LFS bytes.
python3 - "$WORK/origin/payload.bin" <<'PY'
import sys
open(sys.argv[1],'wb').write((b'new-lfs-payload-'*131072)[:1500000])
PY
cp "$WORK/origin/payload.bin" "$WORK/expected-new.bin"; git -C "$WORK/origin" add payload.bin; git -C "$WORK/origin" commit -m changed >/dev/null; git -C "$WORK/origin" push fixture main >/dev/null; git -C "$WORK/origin" lfs push fixture --all >/dev/null
FROZEN=$("${A[@]}" fetch payload --frozen); [[ "$FROZEN" == "$OLD" ]]; "${A[@]}" artifact materialize "$FROZEN" "$WORK/frozen.bin"; cmp "$WORK/expected-old.bin" "$WORK/frozen.bin"
NEW=$("${A[@]}" fetch payload); [[ "$NEW" != "$OLD" ]]; "${A[@]}" artifact materialize "$NEW" "$WORK/new.bin"; cmp "$WORK/expected-new.bin" "$WORK/new.bin"
python3 - "$WORK/Artifactum.lock" <<'PY'
import sys,re
text=open(sys.argv[1]).read(); assert 'lfs_oid' in text and 'commit' in text and 'resolution_json' in text
print('lock carries Git commit and LFS object identity')
PY
echo "Git LFS provider passed: old=$OLD new=$NEW"
