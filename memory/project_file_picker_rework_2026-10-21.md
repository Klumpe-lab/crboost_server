---
name: project-file-picker-rework-2026-10-21
description: ui/local_file_picker.py rewritten as the one compact browser for all 15 pickers; root limit + multi-select removed; NOT RUN
metadata:
  type: project
---

2026-10-21 (branch `segmentations` working tree, uncommitted): `local_file_picker` rewritten. The path bar IS the
autocomplete (text after the last `/` filters the listed folder; Tab completes, ↑↓ highlight in-browser, Enter opens);
pasted paths go straight there, missing paths land on the nearest existing folder with a red footer note; pasted
`…/*.eer` sets the editable glob field. Rows are one `ui.html` string (500 cap) with delegated click/dblclick JS.
`upper_limit` (the "root limit" toast on landing/new project/base location) and unused `multiple` were deleted;
pickers that started at `/` (species prompt, template import, mask import) now pass None = last pick's folder.
Select rule (user-requested generalization): highlighted row → exact typed name → the listed folder itself when the picker takes folders; the button names its target ("Select tomograms/", "Open x/"); clicking empty list space clears the highlight. Row icons = flat slate Material glyphs like the header buttons.
CSS `.cb-fp-row` hover/selected in ui/dashboard/css.py. Only ruff verified — runtime check owed.
`ui/glob_directory_input.py` (landing movie/mdoc glob field) left untouched.
