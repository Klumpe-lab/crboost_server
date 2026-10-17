"""Headers shared by the routes that serve files.

NiceGUI installs a GZipMiddleware (level 9) that compresses every response without a
`Content-Encoding`, in the event loop. Images and volumes do not shrink under gzip, and a volume
of hundreds of MB would hold the loop for seconds, so every route that serves binary data sends
`NO_GZIP`; a large JSON file does too (`json_headers`).
"""

NO_GZIP = {"Content-Encoding": "identity"}

# Below this a JSON file is compressed: cheap, and it travels faster over an SSH tunnel.
_GZIP_JSON_MAX_BYTES = 1 << 20


def json_headers(size: int, cache_control: str) -> dict[str, str]:
    """Headers for a JSON file of `size` bytes: compressed when small, sent as is when large."""
    headers = {"Cache-Control": cache_control}
    if size > _GZIP_JSON_MAX_BYTES:
        headers.update(NO_GZIP)
    return headers
