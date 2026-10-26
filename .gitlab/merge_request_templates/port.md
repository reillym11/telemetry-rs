## Summary

<!-- Which legacy job this ports, which telemetry-core version it uses, and anything new in telemetry-core. -->

## Semantics contract

<!-- Link jobs/<job>/SEMANTICS.md. Name the legacy source files and tests it was derived from. -->

- Input topics and keys:
- Output topic or path and key:
- Ordering, dedup and windowing rules:
- State stores and expiry:

## Parity report

<!-- Link the "Parity report" artifact of this MR's pipeline. Paste the check table. -->

| Fixture | Chaos | Result | Report |
|---|---|---|---|
| sample | `--chaos` (CI) | | |
| full | none | | |
| full | `--chaos-at 0.4` | | |
| full | `--chaos-at 0.75` | | |

## Chaos results

| Run | Kill at committed offset | Aborted records (read_uncommitted) | Duplicates after restart |
|---|---|---|---|
| | | | |

## Privacy manifest and state stores

<!-- Link jobs/<job>/privacy-manifest.yaml. List each state store, its sensitive fields, and where the legacy job kept the same state. -->

## Behavioral differences

<!-- Every difference in effective output from the legacy job, or "None". -->

## Assumptions and open questions

-

## Reviewer checklist

- [ ] SEMANTICS.md matches the legacy code, not only its docs
- [ ] Ties, duplicates and late events have unit tests
- [ ] Output validates against the vendored schema
- [ ] Parity passes on sample (CI) and full (all three runs above)
- [ ] E4 passes with the committed privacy manifest
- [ ] No VIN, location or record value reaches a log line
- [ ] `contracts/SOURCE` SHAs match the vendored files
- [ ] Helm values added under `charts/telemetry-job/values/`
