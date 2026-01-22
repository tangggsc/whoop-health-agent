import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("WHOOP_CLIENT_ID")
CLIENT_SECRET = os.getenv("WHOOP_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("WHOOP_REFRESH_TOKEN")

def get_access_token():
    resp = requests.post(
        "https://api.prod.whoop.com/oauth/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": REFRESH_TOKEN,
            "scope": "offline",
        },
        auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET),
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

if __name__ == "__main__":
    print("Access token acquired")