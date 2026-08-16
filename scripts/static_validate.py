#!/usr/bin/env python3
"""No-toolchain structural validation. This is supplementary, never a replacement for cargo."""
from __future__ import annotations
import re, sys, tomllib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
errors=[]
# Every manifest must parse and workspace crates must have a source target.
for manifest in [ROOT/'Cargo.toml', *sorted((ROOT/'crates').glob('*/Cargo.toml'))]:
    try: data=tomllib.loads(manifest.read_text())
    except Exception as e: errors.append(f'{manifest}: TOML: {e}'); continue
    if manifest.parent.name!='artifactum-work' and manifest!=ROOT/'Cargo.toml':
        if not (manifest.parent/'src/lib.rs').exists() and not (manifest.parent/'src/main.rs').exists(): errors.append(f'{manifest.parent}: no src/lib.rs or src/main.rs')
# Path dependencies must resolve.
for manifest in sorted((ROOT/'crates').glob('*/Cargo.toml')):
    data=tomllib.loads(manifest.read_text())
    for section in ('dependencies','dev-dependencies','build-dependencies'):
        for name,spec in data.get(section,{}).items():
            if isinstance(spec,dict) and 'path' in spec:
                target=(manifest.parent/spec['path']).resolve()
                if not (target/'Cargo.toml').exists(): errors.append(f'{manifest}: {name} path dependency missing: {target}')
# Provider crates (except SDK helpers) must have executable targets.
for manifest in sorted((ROOT/'crates').glob('artifactum-provider-*/Cargo.toml')):
    if manifest.parent.name in {'artifactum-provider-sdk','artifactum-provider-api','artifactum-provider-command','artifactum-provider-opendal','artifactum-provider-testkit'}: continue
    if not (manifest.parent/'src/main.rs').exists(): errors.append(f'{manifest.parent.name}: provider lacks plugin executable')
# Lightweight delimiter scanner with Rust-aware string/comment stripping. This is
# deliberately conservative around lifetimes: a single quote is treated as a
# character literal only when a closing quote appears within a short literal span.
def strip_rust_noncode(text: str) -> str:
    out=[]; i=0; n=len(text); block_depth=0
    while i<n:
        if block_depth:
            if text.startswith('/*', i): block_depth+=1; i+=2; continue
            if text.startswith('*/', i): block_depth-=1; i+=2; continue
            out.append('\n' if text[i]=='\n' else ' '); i+=1; continue
        if text.startswith('//', i):
            j=text.find('\n',i+2)
            if j<0: out.extend(' '*(n-i)); break
            out.extend(' '*(j-i)); out.append('\n'); i=j+1; continue
        if text.startswith('/*', i): block_depth=1; out.extend('  '); i+=2; continue
        # raw strings: r"...", r#"..."#, br#"..."#
        raw_start=None
        for prefix in ('br','r'):
            if text.startswith(prefix,i):
                k=i+len(prefix)
                while k<n and text[k]=='#': k+=1
                if k<n and text[k]=='"': raw_start=(k-i-len(prefix), k, k-(i+len(prefix))); break
        if raw_start is not None:
            _, quote, hashes=raw_start; close='"' + ('#'*hashes); j=text.find(close,quote+1)
            if j<0: out.extend(' '*(n-i)); break
            segment=text[i:j+len(close)]; out.extend('\n' if c=='\n' else ' ' for c in segment); i=j+len(close); continue
        # normal/byte string
        if text[i]=='"' or (text.startswith('b"',i)):
            j=i+(1 if text[i]=='"' else 2); esc=False
            while j<n:
                c=text[j]
                if esc: esc=False
                elif c=='\\': esc=True
                elif c=='"': j+=1; break
                j+=1
            segment=text[i:j]; out.extend('\n' if c=='\n' else ' ' for c in segment); i=j; continue
        # Character literal (but not a Rust lifetime such as 'a or '_).
        if text[i]=="'":
            j=i+1; esc=False; close=None
            while j<min(n,i+10):
                c=text[j]
                if esc: esc=False
                elif c=='\\': esc=True
                elif c=="'": close=j; break
                elif c=='\n': break
                j+=1
            if close is not None:
                segment=text[i:close+1]; out.extend(' '*len(segment)); i=close+1; continue
        out.append(text[i]); i+=1
    return ''.join(out)

for src in sorted((ROOT/'crates').glob('*/src/*.rs')):
    text=strip_rust_noncode(src.read_text())
    stack=[]; pairs={')':'(',']':'[','}':'{'}
    failed=False
    for i,ch in enumerate(text):
        if ch in '([{': stack.append((ch,i))
        elif ch in pairs:
            if not stack or stack[-1][0]!=pairs[ch]:
                errors.append(f'{src}: delimiter mismatch near byte {i}'); failed=True; break
            stack.pop()
    if not failed and stack: errors.append(f'{src}: unclosed delimiters {[x[0] for x in stack[-8:]]}')
# Guard invariants that were easy to accidentally regress.
checks={
 'store separates artifact/content': ('crates/artifactum-store/src/lib.rs','artifacts_dir'),
 'streaming file hash': ('crates/artifactum-store/src/lib.rs','async fn hash_file'),
 'chunking': ('crates/artifactum-store/src/lib.rs','import_chunked_blob_artifact'),
 'daemon host': ('crates/artifactum-plugin-host/src/lib.rs','maybe_run_daemon'),
 'action attempts': ('crates/artifactum-engine/src/lib.rs','AttemptRecord'),
 'collections map': ('crates/artifactum-pipeline/src/lib.rs','foreach'),
 'native remote': ('crates/artifactum-remote/src/lib.rs','pub async fn serve'),
 'OCI export': ('crates/artifactum-provenance/src/lib.rs','export_oci'),
}
for label,(file,needle) in checks.items():
    if needle not in (ROOT/file).read_text(): errors.append(f'missing invariant: {label}')
if errors:
    print('STATIC VALIDATION FAILED',file=sys.stderr)
    for e in errors: print(' -',e,file=sys.stderr)
    sys.exit(1)
print(f'static validation ok: {len(list((ROOT/"crates").glob("*/Cargo.toml")))} crates')
