# AutoPCB Routing Integration Boundary

Date: 2026-08-19

Artifactum should be the artifact lifecycle layer for AutoPCB routing experiments, datasets, native-tool reports, minimized failures, and reproducibility evidence.

## Intended AutoPCB uses

- immutable board snapshots;
- route-task inputs;
- route-event JSONL logs;
- verifier certificates;
- native DRC reports;
- external solver outputs;
- benchmark result tables;
- minimized failure cases;
- model-training datasets;
- exported board/native-tool round trips.

## Boundary rule

Artifactum owns content addressing, lineage, cache semantics, execution receipts, and materialization.  AutoPCB owns board semantics and route validity.

```text
RouteTask + Toolchain + Config
    -> Artifactum ActionSpec
    -> immutable run artifacts
    -> AutoPCB evaluator reads artifacts
    -> promotion only after verification policy passes
```

## Required artifact classes

AutoPCB should eventually store:

- `board-snapshot/v1`;
- `compiled-board/v1`;
- `route-task/v1`;
- `route-session-log/v1`;
- `route-certificate/v1`;
- `native-drc-report/v1`;
- `minimized-routing-failure/v1`;
- `routing-benchmark-report/v1`;
- `routing-dataset-split/v1`.

## Promotion policy

A route artifact must not be promoted as verified unless its trust policy requires:

1. AutoPCB full verification passed;
2. unsupported hard-rule count is zero;
3. deterministic replay passed;
4. native-tool DRC passed when configured;
5. toolchain and input hashes are recorded.

## First implementation slice

Once AutoPCB emits route-session artifacts directly, define an `Artifactum.toml` example pipeline that runs:

```text
compile-board -> route -> verify -> native-oracle -> minimize-failures -> report
```

The pipeline should cache deterministic stages but treat native-tool execution as an effectful action that yields immutable receipt artifacts.
