# Execution semantics

## Action / attempt / realization

An `ActionSpec` says what computation is requested. `ActionKey` identifies that computation. `AttemptRecord` records one execution. `Realization` exists only after success and binds named outputs to immutable artifacts.

Running the same key twice is useful evidence. For a pure action, two different output artifact IDs are a determinism violation.

## Cache policies

- `pure`: expected hermetic/deterministic; reuse successful realization and enforce deterministic history.
- `reproducible`: expected key-complete but may not be fully enforceable; reuse successful realization and allow explicit determinism audits.
- `volatile`: always execute; still preserve immutable outputs/provenance.
- `effect`: always execute. If no data outputs are declared, Artifactum creates an immutable effect-receipt artifact with attempt/log references.

## Sandboxes

Inputs and code are materialized read-only beneath `in/` and `code/`. Declared outputs receive paths beneath `out/`. Templates include:

```text
{in.NAME}
{code.NAME}
{out.NAME}
```

Environment variables include `ARTIFACTUM_ACTION_KEY`, `ARTIFACTUM_TMPDIR`, `ARTIFACTUM_INPUT_*`, `ARTIFACTUM_OUTPUT_*`, and checkpoint paths.

## Checkpoints

Before execution, the newest checkpoints for the same action key are materialized under `ARTIFACTUM_CHECKPOINT_IN`. An action may write recoverable state under `ARTIFACTUM_CHECKPOINT_OUT`. Artifactum captures that directory after normal exit and also after executor errors/timeouts where the sandbox remains available. A retry receives those checkpoint artifacts automatically.

## Budgets and metrics

Attempts record wall time and executor-estimated cost. `max_wall_seconds` constrains execution timeout; `max_usd_micros` rejects a realization when measured/estimated cost exceeds policy. Resource reservation itself is not part of `ActionKey`.

## Cancellation

A running attempt has a cancellation control file. `artifactum runs cancel <attempt-uuid>` requests cancellation; the local process executor kills its child and records a failed attempt rather than a realization.
