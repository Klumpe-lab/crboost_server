---
name: feedback-no-cluster-workaround-automation
description: "Don't build automation that works around cluster-infra failures (auto-retry with node exclusion, node health probing) — static exclude + report to admins instead"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 9456622c-f6c5-4da1-95bc-97d80724c145
  modified: 2026-08-14T12:25:03.266Z
---

When CBE node rot kills jobs (see [[cbe-node-apptainer-mount-rot]]), do NOT propose or build self-healing machinery in crboost — supervisor auto-retry with harvested node excludes was rejected 2026-08-14 as a "cluster-crawling abomination".

**Why:** Cluster infra failures are the admins' problem; encoding workarounds into crboost adds complexity that papers over rot instead of getting it fixed, and the user prefers keeping the pipeline code simple over resilience-to-broken-nodes.

**How to apply:** On node-rot failures: (1) extend the static `--exclude` in config/qsub.sh:11, (2) hand the user a concrete evidence summary to send to CBE admins (node names, FATAL lines, stale job IDs, dates). Nothing more.
