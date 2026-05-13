# Molstar Viewer — Improvement Plan (v2, 2026-05-12)

_Rewritten after discovering the source repo. Previous version
incorrectly assumed `static/molstar/embed.js` was an opaque 2.5 MB
artifact we could only patch. We own the TypeScript source._

## Source of truth

**Source repo:** `/users/artem.kushner/dev/template-workbench/`
- `src/embed/bridge.ts` — postMessage protocol, command dispatcher.
- `src/embed/index.ts` — entry point that mounts the bridge.
- `src/lib/core/AlignmentViewer.ts` — main viewer class
  (`loadLocalVolume`, `setMapIsoValue`, transforms, lifecycle).
- `src/lib/core/representations.ts` — iso-surface params,
  `getVolumeStats`, abs/rel iso conversions.
- `src/lib/core/postprocessing.ts` — `STYLIZED_POSTPROCESSING`.
- `src/lib/{types,utils,index}.ts` — shared types and helpers.
- `package.json` — declares `molstar ^4.0.0`. Build is Vite + TS.
- `dist/embed.js` — byte-identical to `static/molstar/embed.js`.

**Build chain:**

```bash
cd /users/artem.kushner/dev/template-workbench
npm run build                  # produces dist/embed.js
cp dist/embed.js /users/artem.kushner/dev/crboost_server/static/molstar/embed.js
```

That's the entire deploy. After overwriting, restart `python main.py`
(NiceGUI doesn't hot-reload). The iframe pulls the new bundle on next
mount.

## Symptoms (in priority order)

1. **σ-normalized resampled templates (e.g. `..._apix6.20_box96_white.mrc`,
   3 MB, mean=0, sigma=1, 15% of voxels above iso=1.5) don't render
   in the canvas.** Sidebar entry appears via `itemsChanged`, ISO
   slider does nothing across full range [0.5, 5.0]. The basic-shape
   ellipsoid template (smooth, std=1, comparable size) renders fine.
   Original 1.55 Å/px 345 MB template renders sometimes / partially.

2. **Black-polarity template never renders.** Suspected cause traced
   to `AlignmentViewer.loadLocalVolume` lines 282–288:

   ```ts
   const isInverted = stats.max < Math.abs(stats.min) * 0.5;
   ```

   For our normalized data the black template has min≈-2.7, max≈+4.4
   (outlier voxels in the negative→positive map). The heuristic checks
   `max < |min|*0.5` which evaluates FALSE for both polarities, so the
   black template's `actualIsoValue` stays positive — no negative
   isosurface drawn, and the +iso surface has no support either.

3. **Visibility toggles work half the time.** Confirmed in production.
   Slice A (Python optimistic UI + dedup) helps; underlying race in
   `setSubtreeVisibility` not yet investigated.

4. **No load progress feedback.** `load_volume` returns `mapLoaded` on
   completion only. For a multi-100-MB volume the user sees the
   sidebar entry instantly (our Slice C optimistic placeholder) but
   no feedback during the slow path.

## What the bridge already gives us

Reading `bridge.ts` and the deployed bundle confirms:

- ✅ `{ type: 'ready' }` event on init.
- ✅ `{ type: 'itemsChanged', items }` event on every state change.
- ✅ `{ type: 'error', action, message }` event with action context.
  The Python side already consumes this (Slice A enriched the log
  format). **Verified emitting in the compiled bundle** — the catch
  block at the end of the command switch posts it.
- ✅ `{ type: 'mapLoaded' | 'structureLoaded', item }` per-load event.
  Python doesn't read these today; could remove the redundancy or use
  them for cleaner correlation.

What's NOT emitted today:
- No `volume_loading_progress` mid-stream events.
- No `meshGenerated` / `meshFailed` event after iso-surface compute.
- No `clear_complete` after `clear` lands.

## Plan — three slices

### Slice B1 — Diagnostic logging (1–2 hours, no behavior change)

Goal: figure out which step silently fails on the resampled volume
before doing anything else.

Edit `src/lib/core/AlignmentViewer.ts:loadLocalVolume` and
`src/embed/bridge.ts` to emit fine-grained events:

```ts
// In bridge.ts — new event types:
| { type: 'loadProgress'; itemId: string; stage: 'download' | 'parse' | 'represent' | 'done' | 'error'; detail?: string }
| { type: 'volumeStats'; itemId: string; stats: VolumeStats; isInverted: boolean }
```

In `loadLocalVolume`:
1. Emit `loadProgress(stage='download')` before `Data.Download`.
2. Emit `loadProgress(stage='parse')` before `ccp4Format.parse`.
3. Emit `volumeStats(stats, isInverted)` after `getVolumeStats`.
4. Emit `loadProgress(stage='represent')` before
   `createVolumeRepresentation`.
5. Emit `loadProgress(stage='done')` at the end (or `error` from the
   existing catch).

Python side: extend `_handle_viewer_event` in
`ui/template_workbench.py` to display these in the Activity Log so
we can see the stage of failure in real time. The per-card pending
spinners we added in Slice C can advance / flip to error based on
these events.

After this lands and we re-test the resampled-white template, we'll
know whether the failure is in download, parse, stats, or
representation. Likely culprits in priority:
- `representations.ts:createVolumeRepresentation` — uses
  `Volume.IsoValue.relative(isoValue)` which then becomes
  `mean + isoValue * sigma`. If the parsed volume's `grid.stats` are
  wrong (CCP4 header dmin/dmax/dmean/rms not picked up), the iso math
  is off.
- Mesh generation with `alpha: 0.4` and a noisy RELION class may
  produce too many triangles → GPU OOM → silent failure.

### Slice B2 — Fix the polarity-detection heuristic

`isInverted` at `AlignmentViewer.ts:282` only fires when
`max < |min|*0.5`. That's wrong for our normalized data where outliers
on either side push the absolute min/max away from the signal location.

Two fixes to consider:
1. **Pass the polarity from Python.** The workbench knows whether the
   user clicked the white or black card. Extend `load_volume` action
   schema with `polarity: 'white' | 'black'`. The bridge passes
   `actualIsoValue = polarity === 'black' ? -|iso| : +|iso|`. Removes
   the heuristic entirely.
2. **Better heuristic: use the bulk position.** If `mean < 0` and
   `max > |min|`, signal is positive (white). If `mean > 0` and
   `|min| > max`, signal is negative (black). For mean≈0 (normalized)
   the heuristic must rely on something else — bias toward the value
   with the larger magnitude:
   `isInverted = Math.abs(stats.min) > Math.abs(stats.max) * 1.1`.

Option 1 is cleaner — the workbench's `ParticleTemplate.polarity` is
the authoritative source. Plumb it through.

### Slice B3 — Per-volume slicing API

Wire molstar's clip-plane representation into three new bridge
commands:

```ts
| { action: 'setSliceAxis'; itemId: string; axis: 'x' | 'y' | 'z' | 'none' }
| { action: 'setSlicePosition'; itemId: string; position: number }   // [0, 1]
| { action: 'setSliceMode'; itemId: string; mode: 'off' | 'clip' | 'slab' }
```

Implementation: add a representation-update step in
`representations.ts` that toggles a clip-plane on the iso-surface.
Python side: render three sliders + axis dropdown in the session-tray
entry when a volume is the active item.

### Slice B4 — Size guard + memory cleanup

In bridge `load_volume`:

```ts
// HEAD the URL or peek the MRC header for size; reject above threshold.
const sizeMb = await peekSize(cmd.url);
if (sizeMb > MAX_LOAD_MB) {
    emit({ type: 'error', action: 'load_volume',
           message: `Volume is ${sizeMb} MB; max ${MAX_LOAD_MB} MB. Resample first.` });
    return;
}
```

In `clear`: verify `PluginCommands.State.RemoveObject` actually
releases GPU memory. If not, add explicit `plugin.canvas3d?.requestDraw`
or a dispose step.

## What I'm NOT planning yet

- ❌ Switching to a different viewer (Three.js, mol*, etc.). Molstar
  works for the ellipsoid; we just need to make it work for our
  RELION-class templates.
- ❌ Rewriting `loadVolumeFromUrl` to match `loadLocalVolume`. The
  two paths have diverged but both are in use. Out of scope.
- ❌ Adding structure-volume alignment features.

## Test plan after this lands

1. Resample original → load white in viewer → verify
   `loadProgress(stage='done')` arrives → verify isosurface drawn.
2. Load black → verify negative iso applied → verify negative-side
   isosurface drawn.
3. Spam-click visibility → verify no UI/state drift.
4. Click `Reset viewer` mid-load → verify iframe remounts cleanly.
5. Load the 345 MB original → either renders or surfaces
   `too_large` error (currently silently stalls).

## Open decisions for the next molstar session

1. **Mesh quality vs. memory.** Molstar's default iso-surface mesh
   may be too dense for noisy RELION classes. Add a `meshResolution`
   knob alongside `isoValue`?
2. **CCP4 parser pickup of MRC header stats.** Worth verifying the
   parsed `grid.stats` matches the file's dmin/dmax/dmean/rms. If
   it's falling back to the `{ -1, 1, 0, 0.1 }` default, that alone
   explains the iso-value miscalibration.
3. **Camera framing.** Does molstar auto-zoom to fit the new volume?
   If a 96-box volume is loaded after the first one was 128-box, is
   the camera framed correctly? Worth a `cameraReset` action.
