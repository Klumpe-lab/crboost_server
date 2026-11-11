"""A project snapshot: the workspace's views frozen as static pages, zipped for a collaborator to open in a
browser without a server. `routes.py` serves the capture (`capture.js`, driven by `plan.py`); the frozen
pages carry `frozen.js` / `frozen.css`; `services/snapshot/bundle.py` writes the zip."""
