---
name: feedback_engage_concrete_goal
description: "When the user says \"fix X functionality\", pin the concrete success use case FIRST — don't lead with an abstract scope/approach menu. Concrete UX forks (with previews) are fine; strategic scope buckets are not."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b3df5f19-e3ea-49c7-b7a6-a26be8fac4c9
---

When the user asks to "fix the X functionality," they think in **concrete workflows / success use
cases**, not in implementation-scope buckets. Pin the end-to-end happy path they're chasing first, then
diagnose + propose against THAT.

**Why:** 2026-06-25, on the cross-project aggregation work, I presented a 4-option AskUserQuestion *scope*
menu (low-regret fixes / framing rework / start P3 / reactive-tree). The user **rejected** it: "what I
actually care about here is being able to pull in optsets from multiple tomos/projects and use them for
extraction (or if already extracted, reconstruction)… right now this workflow is awful," then
pinned the exact case (10 projects of already-extracted filtered picks → aggregate → reconstruct).

**How to apply:**
- After "fix X," first extract/confirm the concrete success use case (one sentence: "from <inputs> →
  <minimal steps> → <outcome>"). Diagnose and propose against that specific path.
- Multiple-choice IS welcome for a **concrete fork the user can evaluate** — e.g. the explorer-layout
  AskUserQuestion earlier the same session (two ASCII-preview layouts) was answered happily. The
  difference is concrete-thing-to-pick vs abstract-scope-to-authorize.
- Don't ask the user to authorize *scope* ("how much should I do / which approach") before the goal is
  pinned — that reads as offloading the thinking. Pin the goal, then recommend, then act.

See [[user_artem]], [[project_picks_filter_aggregation]].
