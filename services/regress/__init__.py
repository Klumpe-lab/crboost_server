"""Regression harness over protocols (roadmap 14, Tier B + Tier A).

    inputs.py    assertion #0: the frozen dataset (test/input.yaml) + download commands
    checks.py    per-stage metric extractors
    bands.py     bands.yaml load / evaluate / propose (bless)
    infra.py     INFRA signature table + node lookup
    report.py    run.json / metrics.json / report.md, run listing, retention
    runner.py    run_protocol(): apply -> deploy -> wait -> checks -> verdicts; record; bless
    snapshot.py  Tier A: materialized-scheme snapshots (no cluster)

Headless: no ui imports. The CLI is crboost_regress.py at the repo root.
"""
