#!/usr/bin/env python3
"""Structural checks usable in environments without a Rust toolchain."""
from __future__ import annotations

import glob
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ERRORS: list[str] = []


def error(message: str) -> None:
    ERRORS.append(message)


def load_toml(path: Path):
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except Exception as exc:
        error(f"{path.relative_to(ROOT)}: TOML parse failed: {exc}")
        return {}


manifests = sorted(ROOT.glob("**/Cargo.toml"))
parsed = {p: load_toml(p) for p in manifests}

# Workspace coverage.
root = parsed[ROOT / "Cargo.toml"]
members: set[Path] = set()
for pattern in root.get("workspace", {}).get("members", []):
    for match in glob.glob(str(ROOT / pattern)):
        path = Path(match)
        if (path / "Cargo.toml").exists():
            members.add(path.resolve())
crate_dirs = {p.parent.resolve() for p in manifests if p != ROOT / "Cargo.toml"}
for missing in sorted(crate_dirs - members):
    error(f"workspace does not cover crate {missing.relative_to(ROOT)}")

# Path dependencies resolve.
for manifest, data in parsed.items():
    for table_name in ("dependencies", "dev-dependencies", "build-dependencies"):
        for name, spec in data.get(table_name, {}).items():
            if isinstance(spec, dict) and "path" in spec:
                target = (manifest.parent / spec["path"]).resolve()
                if not (target / "Cargo.toml").exists():
                    error(f"{manifest.relative_to(ROOT)}: path dependency {name} -> {target} is missing")
    for target_table in data.get("target", {}).values():
        for table_name in ("dependencies", "dev-dependencies", "build-dependencies"):
            for name, spec in target_table.get(table_name, {}).items():
                if isinstance(spec, dict) and "path" in spec:
                    target = (manifest.parent / spec["path"]).resolve()
                    if not (target / "Cargo.toml").exists():
                        error(f"{manifest.relative_to(ROOT)}: target path dependency {name} is missing")

# Provider wave completeness / dual library+binary form.
provider_names = {
    "local", "http", "github", "huggingface",
    "oci", "s3", "git", "dvc", "kaggle", "modelscope", "ngc", "mlflow", "wandb", "lakefs",
    "gitlab", "gcs", "azure", "zenodo", "figshare", "osf", "dataverse", "clearml", "comet", "ipfs",
    "sftp", "webdav", "ftp", "hdfs", "webhdfs", "gdrive", "onedrive", "dropbox", "swift", "oss", "obs", "cos",
}
for name in sorted(provider_names):
    crate = ROOT / "crates" / f"artifactum-provider-{name}"
    if not crate.exists():
        error(f"missing provider crate: {name}")
        continue
    for source in (crate / "src/lib.rs", crate / "src/main.rs"):
        if not source.exists():
            error(f"{crate.name}: missing {source.name}")
    main = crate / "src/main.rs"
    if main.exists() and "artifactum_plugin_protocol::serve" not in main.read_text(errors="replace"):
        error(f"{crate.name}: plugin binary does not use common protocol server adapter")

# daemonkit must be pinned, not floating.
host_manifest = parsed.get(ROOT / "crates/artifactum-plugin-host/Cargo.toml", {})
daemonkit = host_manifest.get("dependencies", {}).get("daemonkit", {})
if not isinstance(daemonkit, dict) or "git" not in daemonkit or "rev" not in daemonkit:
    error("artifactum-plugin-host must pin daemonkit with git + rev")

# First-party providers must not persist live HTTP headers in ResolvedFile::source.
for lib in sorted((ROOT / "crates").glob("artifactum-provider-*/src/lib.rs")):
    text = lib.read_text(errors="replace")
    if re.search(r"source\s*:\s*serde_json::json!\([^\n]{0,500}[\"']headers[\"']", text):
        error(f"{lib.relative_to(ROOT)}: resolved source appears to persist HTTP headers")

# Rough delimiter validation after lexical stripping. This catches most generation/truncation mistakes.
def strip_rust(text: str) -> str:
    out: list[str] = []
    i = 0
    n = len(text)
    block_depth = 0
    while i < n:
        if block_depth:
            if text.startswith("/*", i):
                block_depth += 1; i += 2
            elif text.startswith("*/", i):
                block_depth -= 1; i += 2
            else:
                i += 1
            continue
        if text.startswith("//", i):
            j = text.find("\n", i)
            if j < 0: break
            out.append("\n"); i = j + 1; continue
        if text.startswith("/*", i):
            block_depth = 1; i += 2; continue
        # Rust raw strings: r"...", r#"..."#, br##"..."##.
        m = re.match(r"(?:b)?r(#+)?\"", text[i:])
        if m:
            hashes = m.group(1) or ""
            i += m.end()
            end = '"' + hashes
            j = text.find(end, i)
            i = n if j < 0 else j + len(end)
            out.append('""')
            continue
        if text[i] == '"' or (text[i] == 'b' and i + 1 < n and text[i+1] == '"'):
            if text[i] == 'b': i += 1
            i += 1
            while i < n:
                if text[i] == "\\": i += 2; continue
                if text[i] == '"': i += 1; break
                i += 1
            out.append('""'); continue
        # Char literal only if there is a closing quote soon; don't consume lifetimes.
        if text[i] == "'":
            j = i + 1
            if j < n and text[j] == "\\": j += 2
            else: j += 1
            if j < n and text[j] == "'":
                i = j + 1; out.append("''"); continue
        out.append(text[i]); i += 1
    return "".join(out)

pairs = {"{": "}", "(": ")", "[": "]"}
closers = {v: k for k, v in pairs.items()}
for source in sorted(ROOT.glob("crates/**/*.rs")):
    stripped = strip_rust(source.read_text(errors="replace"))
    stack: list[tuple[str, int]] = []
    for pos, ch in enumerate(stripped):
        if ch in pairs:
            stack.append((ch, pos))
        elif ch in closers:
            if not stack or stack[-1][0] != closers[ch]:
                error(f"{source.relative_to(ROOT)}: unbalanced delimiter near byte {pos}: {ch}")
                break
            stack.pop()
    else:
        if stack:
            error(f"{source.relative_to(ROOT)}: unclosed delimiter {stack[-1][0]} near byte {stack[-1][1]}")

if ERRORS:
    print(f"static validation FAILED ({len(ERRORS)} error(s))", file=sys.stderr)
    for item in ERRORS:
        print(f"- {item}", file=sys.stderr)
    sys.exit(1)

print(f"static validation OK: {len(manifests)} Cargo manifests, {len(crate_dirs)} workspace crates, {len(provider_names)} concrete providers")
