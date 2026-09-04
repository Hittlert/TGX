# TGX Evaluation Tools

These scripts are offline release-evaluation tools. They are not imported by
the TGX daemon and are excluded from the production Docker build context.

## Canonical entry point

Use `run_protocol_v1.py` to run the suite. It coordinates three distinct
owners:

- `manifest_generator.py`: creates a deterministic, immutable cohort once;
- `harness.py`: executes one isolated engine run and writes sealed raw facts;
- `analyze.py`: reads sealed raw facts and writes policy-versioned analysis.

`self_test.py` exercises these real implementations with temporary fixtures.
It is a prerequisite check, not release evidence.

## Invariants

- An existing manifest or run directory is never overwritten by default.
- TDL and TGX runs consume the same byte-identical manifest.
- The collector never writes a verdict into `raw/`.
- The analyzer never modifies `raw/` and rejects a broken checksum seal.
- Missing measurements are `null` plus `collection_errors`, never fabricated
  zeroes.

Environment-specific NAS paths, session copies, proxy routing, and artifacts
belong in command arguments or RunSpec data, not in production code.

## Usage

Create an environment file from `evaluation-config.example.json`, then run one
explicit phase at a time:

```bash
python3 scripts/evaluation/run_protocol_v1.py self-test
python3 scripts/evaluation/run_protocol_v1.py manifests --config eval.json
python3 scripts/evaluation/run_protocol_v1.py baseline --config eval.json
python3 scripts/evaluation/run_protocol_v1.py candidate --config eval.json
python3 scripts/evaluation/run_protocol_v1.py report --config eval.json
```

The baseline command defaults to one repetition and refuses an already existing
TDL baseline with the same cohort, artifact, concurrency, duration, and warmup.
Use `--force` only for an intentional independent repetition.

The candidate command requires a checksum-valid, policy-GO TDL baseline with
the same cohort and workload identity. TGX performance gates consume that
baseline identity and throughput directly; reports never compare different
cohorts in the same table.

Newly sampled manifests use `baseline_trust=local_disk`. A checksum-valid,
independent TDL run attests each case whose downloaded size and SHA match that
reference. Later TGX runs may claim hard correctness only for cases covered by
the matching TDL attestation; the original manifest remains immutable.

Only `large_*` cases may end at the run timeout without becoming correctness
failures. Small and medium cases must complete with exact size and SHA. A large
case that reports success with a bad hash, ends in engine failure, or leaves
residue still fails the run.

The analyzer intentionally returns `BLOCKED` when a required network, memory,
SSD, or target-storage measurement is unavailable. Add read-only instrumentation
at the owning runtime layer; never replace a missing measurement with zero.
