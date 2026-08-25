git add ui/data_import_panel.py
git commit -m "landing: a project with no raw data can actually be created" -m "on_movies_change derived the co-located mdocs glob only from a
non-empty frames value, so clearing the frames path stranded the last derived *.mdoc pattern in state - and that input is hidden unless 'mdocs
elsewhere' is on, so the form could never reach is_dataless() and Create stayed disabled on 'Missing: Data Path'. The derive now follows the field all
the way to empty. Empty globs read neutral instead of a red field error while BOTH are empty; a half-filled form still demands the other half. Adds a
'No raw data' button that clears both patterns in one click, since prefs pre-fill the fields on every load and 'just leave them empty' was never
actually on offer."

git add services/visualization/tomo_geometry.py
git commit -m "geometry: all_geometries(), every tomogram a project knows about" -m "Same source precedence as tomogram_star_sources (the reconstruct
job's star, then the imported one) and the same row -> TomoGeometry construction as geometry_for_ts, so a tomogram described by both is reported once,
from the row the per-TS lookup would have used. The gallery needs the whole wall at once; asking per name would re-scan each star for every tile."

git add static/icons/tomo_preview.svg ui/dashboard/css.py ui/tomo_gallery.py
git commit -m "gallery: birds-eye wall of reconstructions with species pick overlays" -m "One tile per tomogram from all_geometries, so an
imported-volume project gets a wall too. The image is the WarpTools PNG ts_reconstruct wrote, else our cached X/Y slab, else a background render
kicked with the Journey's own cache paths and dedup keys (capped 6 per pass; the 15 s poll runs only while tiles are pending and only repaints when
the signature moves). Toolbar switches - source (recon plus each denoisepredict method that has a volume for the tomogram), per-species pick toggles,
tile size - re-render from the collected rows and never touch disk. Picks draw as ONE inline SVG per (tile, species): percentage cx/cy with a pixel
radius, so dots keep constant screen size at any tile size and a 40-tomogram wall costs layers instead of thousands of elements. That requires the
frame box to BE the image box (aspect-ratio from the tomogram's own dims, object-fit: fill); with no resolvable extent nothing is drawn and the count
badge goes amber, rather than scattering picks against a guessed size. The image opens the zoom view, the caption opens that tomogram in the Journey.
tomo_preview.svg was an empty leftover from the old Tomogram Previews grid."

git add ui/workspace_page.py ui/pipeline_builder/pipeline_builder_panel.py
git commit -m "workspace: the gallery as a fourth main-area view" -m "gallery_container + _show_gallery beside pipeline / workbench / journey / viewer
- lazily built on first use like the journey, toggling back to the pipeline on a second click of its icon, with set_active driving the page's
pending-preview poll so it only ticks while the wall is visible. toggle_gallery threads through build_pipeline_builder_panel to the roster exactly as
toggle_journey does."

git add ui/pipeline_builder/pipeline_roster.py
git commit -m "sidebar: Tomograms nav icon; the Journey icon is a bar chart" -m "The wall gets its own icon between Particles and Journey, hides the
job roster for full width like the journey and the pick viewer, and lights in set_active_mode. _TOMO_DASHBOARD_SVG's ringed circle said nothing about
what the Journey actually holds - three ascending bars for a surface that carries per-TS progress and statistics. The name stays 'Journey'."


