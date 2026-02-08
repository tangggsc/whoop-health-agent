import os
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, urlparse, parse_qs
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("WHOOP_CLIENT_ID")
CLIENT_SECRET = os.getenv("WHOOP_CLIENT_SECRET")
REDIRECT_URI = os.getenv("WHOOP_REDIRECT_URI", "http://localhost:8000/callback")
SCOPES = os.getenv(
    "WHOOP_SCOPES",
    "read:profile read:cycles read:recovery read:sleep read:workout read:body_measurement",
)
STATE = os.getenv("WHOOP_STATE", "randomstate123")

if not CLIENT_ID or not CLIENT_SECRET:
    raise SystemExit("Missing WHOOP_CLIENT_ID / WHOOP_CLIENT_SECRET in .env")

auth_code = None
oauth_error = None

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code, oauth_error

        parsed = urlparse(self.path)

        # Ignore noise requests (/, /favicon.ico, etc.)
        if parsed.path != "/callback":
            self.send_response(404)
            self.end_headers()
            return

        query = parse_qs(parsed.query)

        # If Whoop returns an OAuth error, show it
        if "error" in query:
            oauth_error = query.get("error", ["unknown"])[0]
            desc = query.get("error_description", [""])[0]
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(f"<h1>OAuth error: {oauth_error}</h1><p>{desc}</p>".encode())
            return

        if "code" in query:
            auth_code = query["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>Success! You can close this window.</h1>")
        else:
            # Don’t fail; keep listening until real callback arrives
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>No code on this request. Please complete the Grant flow.</h1>")

    def log_message(self, format, *args):
        pass

params = {
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "response_type": "code",
    "scope": SCOPES,
    "state": STATE,
}

auth_url = "https://api.prod.whoop.com/oauth/oauth2/auth?" + urlencode(params)

print(f"Listening for callback at {REDIRECT_URI}")
server = HTTPServer(("0.0.0.0", 8000), CallbackHandler)

print("Opening browser for authorization...")
webbrowser.open(auth_url)

print("Waiting for callback... (Ctrl+C to quit)")
while auth_code is None and oauth_error is None:
    server.handle_request()

if oauth_error:
    raise SystemExit(f"OAuth failed: {oauth_error}")

print("Got authorization code! Exchanging for tokens...")

resp = requests.post(
    "https://api.prod.whoop.com/oauth/oauth2/token",
    data={
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    },
)

print("Status:", resp.status_code)
print(resp.text)