# Agent instructions

Artifactum is identity-sensitive infrastructure. Do not make a change that appears to work by bypassing the CAS, source lock, action-key, or provenance invariants.

Before changing code:

1. Read `README.md` and `docs/ARCHITECTURE.md`.
2. Identify which plane owns the behavior: source/resolver, CAS, metadata, engine, executor, remote, or provenance.
3. Preserve the `ContentId` / `ArtifactId` / `ActionKey` distinction.
4. Never put credentials or mutable signed URLs into semantic artifact identity.
5. Never turn a failed attempt into a successful realization.
6. Never use cache hits for `volatile` or `effect` actions.
7. If adding a scheduling-only field, keep it out of `ActionKey` unless it actually changes the computation's observable semantics.

Before submitting changes run `./scripts/validate.sh`, then the relevant focused tests, then `ARTIFACTUM_E2E_KEEP=1 ./scripts/e2e_observe.sh`. Read `AGENT_TESTING.md`: an agent must inspect the produced evidence, not merely report that commands exited zero.
