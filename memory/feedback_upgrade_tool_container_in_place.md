---
name: feedback-upgrade-tool-container-in-place
description: "When one job needs a newer version of a tool crboost already ships (e.g. Warp), upgrade that tool's existing container and tools: key; never add a parallel tool key/image for one job"
metadata:
  node_type: memory
  type: feedback
  originSessionId: f2ccfb83-54d7-4366-8c8a-33aa5f0cfec3
  modified: 2026-10-11T01:09:43.693Z
---

When a single job needs a newer release of a tool that already has a container and `tools:` key (Warp in
`warp_aretomo`), rebuild that container with the new version and point the existing key at it. Do not invent a
second key (`warp_export`) and a second image of the same tool for one job.

**Why:** 2026-10-11, the Warp Export build: I added `tools.warp_export` + a Warp-only dev41 image next to the
dev36 Warp+AreTomo image. The user: "why are you creating a new tool? ... lets just upgrade warp version in that
warp aretomo container". Two versions of one tool in one pipeline is config sprawl and split behaviour.

**How to apply:** Upgrade in place (new def with the bumped version, same bundled companions such as AreTomo),
then check the CLI of every command crboost calls for option changes between the versions and name behaviour
changes from the upstream changelog. [[Separate containers when pinnings differ]] still governs *different*
tools; this is about the same tool. Related [[project_warp_export_job]].
