# ui_elements — rendered CryoBoost UI chrome

Transparent-background PNGs at **5×** (≈360 dpi at slide scale), plus the source SVG of each.
Colors, sizes, fonts and spacing are lifted verbatim from
`ui/pipeline_builder/pipeline_roster.py` + `ui/status_indicator.py`, so these match the running
app rather than approximating it. Type is real IBM Plex Mono / Plex Sans.

## Stage chips — one roster row per pipeline stage, all "done"
Drop one next to each stage box in the lifecycle figure (doc 02). No background, no frame, no
left rail — they composite onto anything.

`stage-importmovies` · `stage-fsMotionAndCtf` · `stage-tiltFilter` · `stage-aligntiltsWarp` ·
`stage-missAlign` · `stage-tsCtf` · `stage-tsReconstruct` · `stage-denoisetrain` ·
`stage-denoisepredict` · `stage-templatematching` · `stage-tmextractcand` ·
`stage-subtomoExtraction` · `stage-reconstructParticle` · `stage-class3d`

(`tsImport` deliberately omitted — you asked to drop it.)

## Rosters
| File | What |
|---|---|
| `roster-all-done` | the full roster, both phase headers, everything green |
| `roster-mixed-live` | a live run: green done, one failed task (`27/28 1!`), TS CTF **running** and row-active with the `▸4 12/28` chip, purple queued, amber scheduled |

## Per-tilt-series tasks  (doc 04's `@ui`)
| File | What |
|---|---|
| `roster-array-running` | the **Reconstruct** job row expanded into its per-TS task list — 6 ok, 4 running, 4 pending |
| `ts-tasks-running` | the same task block with no parent row, if you want to place it yourself |
| `array-task-grid-20` | 20 tasks as a compact tile grid (6 done / 4 running / 10 pending) — for the dispatch figure, where a 20-row list would be too tall |

## Reference
| File | What |
|---|---|
| `status-dot-legend` | all six dot states labeled — Scheduled · Queued · Running · Succeeded · Failed · Orphaned |

## Colors (so LaTeX/Illustrator can match)
```
job dots     scheduled #fbbf24   queued  #a855f7   running #3b82f6
             succeeded #10b981   failed  #ef4444   orphaned #f97316
per-TS task  ok #16a34a   fail #dc2626   running #2563eb   pending #d1d5db
chrome       name #1e293b   sub #64748b   muted #94a3b8
             row rail #e5e7eb (active #475569)   phase header bg #f1f5f9
```

## Regenerating
`_generate.js` is the generator. It needs Node plus `@resvg/resvg-js` and IBM Plex TTFs:

```bash
npm i @resvg/resvg-js @fontsource/ibm-plex-sans @fontsource/ibm-plex-mono wawoff2
# fontsource ships woff2 only; resvg needs sfnt. Decompress once into ./ttf/ as
# PlexSans-{Regular,Medium,SemiBold}.ttf and PlexMono-{Regular,SemiBold}.ttf
# (wawoff2.decompress on the *-latin-{400,500,600}-normal.woff2 files).
node _generate.js <output-dir>
```
Change `SCALE` at the top for a different resolution. Every figure is written as both `.svg`
(vector — better for LaTeX) and `.png`.

**Note:** NiceGUI itself cannot be screenshotted headlessly here — there is no Python
interpreter and no browser on this box. These are hand-built SVGs that reproduce the app's
chrome from its source constants, which is also more reproducible than a screenshot (no live
project, no data, deterministic output).
