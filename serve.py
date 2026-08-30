#!/usr/bin/env python3
"""Render and serve markdown files below this directory as HTML.

Usage:  .venv/bin/python serve.py [port] [host]
Default port: 48217
Default host: 127.0.0.1
"""

from __future__ import annotations

import html
import pathlib
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, unquote, urlsplit

import markdown

HERE = pathlib.Path(__file__).resolve().parent
DEFAULT_PORT = 48217
TITLE = "Personal YScope Docs"

# Markdown extensions used for rendering.
EXTENSIONS = ["tables", "fenced_code", "toc", "sane_lists"]

STYLE = """
body { font: 15px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
       max-width: 920px; margin: 2rem auto; padding: 0 1rem; color: #1f2328; background: #fff; }
h1, h2, h3 { line-height: 1.25; margin-top: 1.5em; }
table { border-collapse: collapse; margin: 1em 0; }
th, td { border: 1px solid #d0d7de; padding: 6px 13px; text-align: left; }
tr:nth-child(even) { background: #f6f8fa; }
code { background: #f6f8fa; padding: .2em .4em; border-radius: 6px; font-size: 85%; }
pre { background: #f6f8fa; padding: 1em; border-radius: 8px; overflow-x: auto; }
pre code { background: none; padding: 0; font-size: 13px; }
blockquote { border-left: 4px solid #d0d7de; margin: 1em 0; padding: 0 1em; color: #57606a; }
a { color: #0969da; }
.index li { margin: .3em 0; }
"""


def render(md_path: pathlib.Path) -> str:
    text = md_path.read_text(encoding="utf-8")
    body = markdown.markdown(text, extensions=EXTENSIONS)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{html.escape(md_path.name)}</title>
<meta name="referrer" content="no-referrer">
<style>{STYLE}</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
</head>
<body><h1>{html.escape(TITLE)}</h1>{body}
<script>
  document.querySelectorAll('pre > code.language-mermaid').forEach(function(code) {{
    const pre = code.parentElement;
    const diagram = document.createElement('div');
    diagram.className = 'mermaid';
    diagram.textContent = code.textContent;
    pre.replaceWith(diagram);
  }});
  if (window.mermaid) {{
    mermaid.initialize({{startOnLoad: false, securityLevel: 'strict'}});
    mermaid.run();
  }}
</script>
</body></html>"""


def index_page() -> str:
    files = sorted(
        path
        for path in HERE.rglob("*.md")
        if not any(part.startswith(".") for part in path.relative_to(HERE).parts)
    )
    items = "".join(
        (
            f'<li><a href="/{quote(path.relative_to(HERE).as_posix())}">'
            f"{html.escape(path.relative_to(HERE).as_posix())}</a></li>"
        )
        for path in files
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{html.escape(TITLE)}</title>
<style>{STYLE}</style></head>
<body><h1>{html.escape(TITLE)}</h1><h2>Documents</h2>
<ul class="index">{items}</ul></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        request_path = unquote(urlsplit(self.path).path)
        if request_path in {"", "/"}:
            self._send(200, index_page())
            return

        relative_path = pathlib.PurePosixPath(request_path.lstrip("/"))
        if ".." in relative_path.parts or any(
            part.startswith(".") for part in relative_path.parts
        ):
            self._send(HTTPStatus.NOT_FOUND, "Not found")
            return

        target = (HERE / pathlib.Path(*relative_path.parts)).resolve()
        try:
            target.relative_to(HERE)
        except ValueError:
            self._send(HTTPStatus.NOT_FOUND, "Not found")
            return
        if not target.is_file() or target.suffix.lower() != ".md":
            self._send(HTTPStatus.NOT_FOUND, "Not found")
            return
        try:
            self._send(200, render(target))
        except Exception as e:  # noqa: BLE001
            print(f"Failed to render {target}: {e}", file=sys.stderr)
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, "Render error")

    def _send(self, status: int, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; "
            "script-src 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'unsafe-inline'; "
            "img-src data:; "
            "connect-src 'none'; "
            "object-src 'none'; "
            "base-uri 'none'; "
            "form-action 'none'; "
            "frame-ancestors 'none'",
        )
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt: str, *args) -> None:  # quieter logs
        sys.stderr.write(f"{self.client_address[0]} - {fmt % args}\n")


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Serving {HERE} on http://{host}:{port}  (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
