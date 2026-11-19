"""File every open roadmap (docs/roadmaps/roadmap_*.md, not completed/) as a GitHub issue and add it
to the lab's project board.

Title: the roadmap's `# ` heading. Body: its row in docs/roadmaps/README.md (goal and status) or,
without a row, its first paragraph, plus a link to the file on main. A roadmap whose title, or
`Roadmap NN` prefix, is already an issue title (open or closed) is skipped, so the script can be
rerun.

Dry run by default; `--apply` creates. Adding to the board needs a token with project access
(`gh auth refresh -s project` for an OAuth login; a fine-grained token needs the organisation's
Projects read/write permission).

    python3 scripts/file_roadmap_issues.py            # print what would be filed
    python3 scripts/file_roadmap_issues.py --apply    # file and add to the board
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = "Klumpe-lab/crboost_server"
OWNER = "Klumpe-lab"
PROJECT_NUMBER = "2"
ROADMAPS = Path(__file__).resolve().parent.parent / "docs" / "roadmaps"
BLOB = f"https://github.com/{REPO}/blob/main/docs/roadmaps"
_NUMBER_RE = re.compile(r"^Roadmap (\d+)\b")


def gh(*args: str) -> str:
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def readme_rows() -> dict[str, tuple[str, str]]:
    """file name → (description, status) from README.md's `| NN | [slug](file) | desc | status |` rows."""
    rows = {}
    for line in (ROADMAPS / "README.md").read_text().splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 4:
            continue
        m = re.search(r"\]\(([^)]+\.md)\)", cells[1])
        if m:
            rows[Path(m.group(1)).name] = (cells[2], cells[3])
    return rows


def first_paragraph(text: str) -> str:
    for block in re.split(r"\n\s*\n", text):
        block = block.strip()
        if block and not block.startswith("#"):
            return block
    return ""


def roadmap_issues() -> list[tuple[str, str]]:
    rows = readme_rows()
    out = []
    for path in sorted(ROADMAPS.glob("roadmap_*.md")):
        text = path.read_text()
        heading = next((ln[2:].strip() for ln in text.splitlines() if ln.startswith("# ")), path.stem)
        title = heading.replace(" — ", ": ", 1)
        if path.name in rows:
            desc, status = rows[path.name]
            body = f"{desc}\n\n**Status:** {status}"
        else:
            body = first_paragraph(text)
        body += f"\n\nRoadmap: [{path.name}]({BLOB}/{path.name})"
        out.append((title, body))
    return out


def existing_titles() -> list[str]:
    issues = json.loads(gh("issue", "list", "-R", REPO, "--state", "all", "--limit", "1000", "--json", "title"))
    return [i["title"] for i in issues]


def already_filed(title: str, existing: list[str]) -> bool:
    m = _NUMBER_RE.match(title)
    for t in existing:
        if t.lstrip("# ").strip() == title or t.lstrip("# ").strip().replace(" — ", ": ", 1) == title:
            return True
        if m and (n := _NUMBER_RE.match(t.lstrip("# ").strip())) and n.group(1) == m.group(1):
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="create the issues and add them to the board")
    args = parser.parse_args()

    existing = existing_titles()
    todo = [(t, b) for t, b in roadmap_issues() if not already_filed(t, existing)]
    skipped = len(roadmap_issues()) - len(todo)
    print(f"{len(todo)} to file, {skipped} already filed")
    for title, body in todo:
        if not args.apply:
            print(f"\n=== {title}\n{body}")
            continue
        url = gh("issue", "create", "-R", REPO, "--title", title, "--body", body).strip()
        print(f"filed {url}  {title}")
        try:
            gh("project", "item-add", PROJECT_NUMBER, "--owner", OWNER, "--url", url)
        except subprocess.CalledProcessError as e:
            print(f"  not added to the board: {e.stderr.strip()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
