import http.server
import socketserver
import urllib.request
import urllib.error
import ssl
import os

PORT = 8000

class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # FIX 1: Strip query strings (e.g. ?v=4) so the server finds the actual file
        if '?' in self.path:
            self.path = self.path.split('?')[0]

        # 1. Intercept requests for PDBs
        if self.path.startswith('/api/pdb/'):
            pdb_id = self.path.split('/')[-1]
            remote_url = f"https://models.rcsb.org/{pdb_id}.bcif"
            print(f"Proxying PDB: {remote_url}")
            self._proxy_request(remote_url)
            return

        # 2. Intercept requests for EMDB
        elif self.path.startswith('/api/emdb/'):
            emdb_id = self.path.split('/')[-1]
            remote_url = f"https://ftp.ebi.ac.uk/pub/databases/emdb/structures/EMD-{emdb_id}/map/emd_{emdb_id}.map.gz"
            print(f"Proxying EMDB: {remote_url}")
            self._proxy_request(remote_url)
            return

        # 3. Default: Serve static files
        else:
            self.send_response(200)
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            super().do_GET()

    def _proxy_request(self, url):
        try:
            # FIX 2: Create an unverified SSL context to bypass the cluster's cert issues
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            # Pass the context to urlopen
            with urllib.request.urlopen(url, context=ctx) as response:
                content = response.read()
                
                self.send_response(200)
                # Forward the content type if available, or default to binary
                content_type = response.headers.get('Content-Type', 'application/octet-stream')
                self.send_header('Content-Type', content_type)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(content)

        except urllib.error.HTTPError as e:
            self.send_error(e.code, f"Upstream Error: {e.reason}")
        except Exception as e:
            # Print the actual error to the terminal so we can see it
            print(f"PROXY ERROR: {e}")
            self.send_error(500, f"Proxy Error: {str(e)}")

# Allow address reuse
socketserver.TCPServer.allow_reuse_address = True

print(f"Serving on port {PORT} (SSL Verification Disabled).")
print("Access via your tunnel at http://localhost:8000")

with socketserver.TCPServer(("", PORT), ProxyHandler) as httpd:
    httpd.serve_forever()