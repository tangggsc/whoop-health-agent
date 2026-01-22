import os
from pathlib import Path
import requests
from dotenv import load_dotenv

# Always load repo-root .env
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"

CLIENT_ID = os.getenv("WHOOP_CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("WHOOP_CLIENT_SECRET", "").strip()
REFRESH_TOKEN = os.getenv("WHOOP_REFRESH_TOKEN", "").strip()


def _update_env_refresh_token(env_path: Path, new_refresh_token: str) -> None:
    """
    Updates (or adds) WHOOP_REFRESH_TOKEN in the .env file.
    """
    if not env_path.exists():
        raise RuntimeError(f".env not found at {env_path}")

    lines = env_path.read_text().splitlines()
    out = []
    replaced = False

    for line in lines:
        if line.startswith("WHOOP_REFRESH_TOKEN="):
            out.append(f"WHOOP_REFRESH_TOKEN={new_refresh_token}")
            replaced = True
        else:
            out.append(line)

    if not replaced:
        out.append(f"WHOOP_REFRESH_TOKEN={new_refresh_token}")

    env_path.write_text("\n".join(out) + "\n")


def refresh_tokens() -> tuple[str, str]:
    """
    Returns (access_token, refresh_token) from Whoop.
    Note: Whoop rotates refresh tokens; you must use the newest one next time. :contentReference[oaicite:3]{index=3}
    """
    if not CLIENT_ID:
        raise RuntimeError(f"Missing WHOOP_CLIENT_ID (loaded from {ENV_PATH})")
    if not CLIENT_SECRET:
        raise RuntimeError(f"Missing WHOOP_CLIENT_SECRET (loaded from {ENV_PATH})")
    if not REFRESH_TOKEN:
        raise RuntimeError(f"Missing WHOOP_REFRESH_TOKEN (loaded from {ENV_PATH})")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "offline",  # per Whoop examples :contentReference[oaicite:4]{index=4}
    }

    resp = requests.post(TOKEN_URL, data=data, timeout=30)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Token refresh failed: {resp.status_code}\n"
            f"Response: {resp.text}\n"
            f"Loaded .env from: {ENV_PATH}"
        )

    payload = resp.json()
    access_token = payload.get("access_token")
    new_refresh_token = payload.get("refresh_token")

    if not access_token:
        raise RuntimeError(f"Token response missing access_token: {payload}")
    if not new_refresh_token:
        # Some providers may not rotate every time, but Whoop generally does. :contentReference[oaicite:5]{index=5}
        # If it's absent, fall back to existing refresh token.
        new_refresh_token = REFRESH_TOKEN

    return access_token, new_refresh_token


def get_access_token() -> str:
    """
    Returns an access token and persists the rotated refresh token back into .env.
    """
    access_token, new_refresh_token = refresh_tokens()

    # Persist new refresh token so future runs don't break
    if new_refresh_token != REFRESH_TOKEN:
        _update_env_refresh_token(ENV_PATH, new_refresh_token)

    return access_token


if __name__ == "__main__":
    token = get_access_token()
    print(f"Access token acquired ✅ (len={len(token)})")
    print(f"Refresh token updated in .env ✅ ({ENV_PATH})")