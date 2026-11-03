---
name: project-style-themes-2026-10-22
description: Harbor palette pinned as THE app look (--cb-* vars in css.py _THEME_CSS), columns job layout; picker + other styles removed; controls sweep in progress; NOT RUN
metadata:
  type: project
---
2026-10-22: after three rounds of a live style picker (Mist, Graphite, open/columns/index layouts, Ledger/Ink/Steel, Atelier/Lumen/Frost), the user pinned **Harbor** + the **columns** job-page layout and asked for "more of that across the UI", especially panel headers, small tools/controls, filters and switches ("really disorganized").

Harbor = white panels with #dbe3ec hairline + faint shadow on a #f6f8fb page; light-blue title stripes #edf4fb with #c9dbee rule and 2 px #4a8fd4 tick; view title lines white→#edf3f9 gradient ruled #a9c6e4; ink #12324f; fields #d3dde8 with blue focus ring; navy accent buttons #1f4e79. All in `:root` of `_THEME_CSS` (ui/dashboard/css.py); inline styles carry the same values as var() fallbacks. Picker JS and data-cb-theme removed. Path copy icons → click-to-copy text same day. NOT RUN.


Controls sweep built same day: `icon_button` (ui/components/buttons.py, 20 px, `.cb-icon-btn`, `--danger`, `--armed`) and `panel_head` (_field_styles.py) are THE primitives; ~95 sites converted; segmented active = light-blue + 2 px blue underline; toolbars under view titles = white strip + hairline butted to the title (margin-top -6px); Journey section headers get the stripe; checkbox/toggle/radio accent via global CSS. NOT RUN.

Rejected on the way (don't re-propose): dark charcoal title bars, zinc/near-black hairlines ("oppressive"), panel-less open sheet, sticky section index.
