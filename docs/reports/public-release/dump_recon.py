"""Render the public-release recon workflow's finished lenses (journal.jsonl) as markdown. Re-run to pick up more."""
import json, sys
from pathlib import Path

journal = Path(sys.argv[1])
out = Path(sys.argv[2])
labels, results = {}, {}
for line in journal.read_text().splitlines():
    try:
        d = json.loads(line)
    except json.JSONDecodeError:
        continue
    if d.get("type") == "started":
        labels[d["key"]] = d.get("label", d["key"])
    elif d.get("type") == "result" and isinstance(d.get("result"), dict):
        results[d["key"]] = d["result"]

all_lenses = sorted(set(labels.values()))
done = [labels.get(k, k) for k in results]
md = ["# Public release recon, 2026-11-19 (raw lens results)", "",
      "Read-only recon for the public-release / deployment roadmap (roadmap 63, not yet written).",
      f"Finished lenses: {', '.join(done)}.",
      f"Not finished when this was written: {', '.join(l for l in all_lenses if l not in done) or 'none'}.", ""]
for key, r in results.items():
    md += [f"## {labels.get(key, key)}: {r.get('lens','')}", "", r.get("summary", ""), "", "### Findings", ""]
    for f in r.get("findings", []):
        md += [f"**{f.get('id')} [{f.get('severity')}, effort {f.get('effort')}] {f.get('title')}**", "",
               f.get("detail", ""), "", "Evidence:"]
        md += [f"- {e}" for e in f.get("evidence", [])]
        md += ["", f"Recommendation: {f.get('recommendation','')}", ""]
    if r.get("facts"):
        md += ["### Facts", ""]
        md += [f"- {x.get('claim')} ({x.get('confidence')}; {x.get('source')})" for x in r["facts"]]
        md += [""]
    if r.get("open_questions"):
        md += ["### Open questions", ""]
        md += [f"- {q}" for q in r["open_questions"]]
        md += [""]
out.write_text("\n".join(md))
print(f"{len(results)} lenses -> {out} ({out.stat().st_size} bytes)")
