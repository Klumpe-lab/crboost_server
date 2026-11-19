"""Render the public-release recon lenses as markdown. Re-run to pick up more.

Usage: dump_recon.py <journal.jsonl> <out.md> [rerun.json ...]
The journal holds the lenses the first workflow finished; each rerun.json is one lens re-run on its own
(the same prompt and result shape), which replaces a journal lens of the same name.
"""

import json
import re
import sys
from pathlib import Path

# The repo is public: agents' scratch paths carry a home-derived name, and the lab's group path is written as a role.
SCRUB = [
    (re.compile(r"/tmp/claude-\d+/[^/\s]+/[0-9a-f-]{36}/scratchpad"), "<scratch>"),
    (re.compile(r"/groups/klumpe\b"), "/groups/<lab>"),
    (re.compile(r"~/\.claude[^/\s]*/projects/[^/\s]+/"), "<assistant home>/"),
    (re.compile(r"user_artem\.md"), "user_<maintainer>.md"),
]

journal = Path(sys.argv[1])
out = Path(sys.argv[2])
labels, results = {}, {}
for line in journal.read_text().splitlines():
    try:
        d = json.loads(line)
    except json.JSONDecodeError:  # a line cut off when the session stopped mid-write
        continue
    if d.get("type") == "started":
        labels[d["key"]] = d.get("label", d["key"])
    elif d.get("type") == "result" and isinstance(d.get("result"), dict):
        results[d["key"]] = d["result"]

for extra in sys.argv[3:]:
    r = json.loads(Path(extra).read_text())
    key = "rerun:" + r["lens"]
    labels[key] = "recon:" + r["lens"]
    results = {k: v for k, v in results.items() if labels.get(k) != labels[key]}
    results[key] = r

all_lenses = sorted(set(labels.values()))
done = [labels.get(k, k) for k in results]
missing = [name for name in all_lenses if name not in done]
md = [
    "# Public release recon, 2026-11-19 (raw lens results)",
    "",
    "Read-only recon for the public-release / deployment roadmap (roadmap 63).",
    f"Finished lenses: {', '.join(done)}.",
    f"Not finished when this was written: {', '.join(missing) or 'none'}.",
    "",
]
for key, r in results.items():
    # Two lenses chose ids that clash: hygiene's H1..H17 with HOTFIXES' H-ids, and a second CI-1.
    lens = labels.get(key, key)
    if lens == "recon:hygiene":
        r = json.loads(re.sub(r"\bH(\d+)\b", r"HY\1", json.dumps(r)))
    elif lens == "recon:ci_release_research":
        r = json.loads(re.sub(r"\bCI-1\b", "CIR-1", json.dumps(r)))
    title = r.get("lens", "")
    if key.startswith("rerun:"):
        title = "the same brief, re-run on its own after the first session stopped"
    md += [f"## {labels.get(key, key)}: {title}", "", r.get("summary", ""), "", "### Findings", ""]
    for f in r.get("findings", []):
        md += [
            f"**{f.get('id')} [{f.get('severity')}, effort {f.get('effort')}] {f.get('title')}**",
            "",
            f.get("detail", ""),
            "",
            "Evidence:",
        ]
        md += [f"- {e}" for e in f.get("evidence", [])]
        md += ["", f"Recommendation: {f.get('recommendation', '')}", ""]
    if r.get("facts"):
        md += ["### Facts", ""]
        md += [f"- {x.get('claim')} ({x.get('confidence')}; {x.get('source')})" for x in r["facts"]]
        md += [""]
    if r.get("open_questions"):
        md += ["### Open questions", ""]
        md += [f"- {q}" for q in r["open_questions"]]
        md += [""]
text = "\n".join(md)
for pattern, role in SCRUB:
    text = pattern.sub(role, text)
out.write_text(text)
print(f"{len(results)} lenses -> {out} ({out.stat().st_size} bytes)")
