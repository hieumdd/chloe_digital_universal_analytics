import os
from urllib3.util.retry import Retry

import requests
from requests.adapters import HTTPAdapter

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

def get_headers():
    """Create headers from Credentials

    Returns:
        dict: HTTP Headers
    """

    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }
    with requests.post(
        "https://oauth2.googleapis.com/token",
        params=params,
    ) as r:
        access_token = r.json()["access_token"]
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def get_sessions():
    sessions = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 503, 500],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sessions.mount("https://", adapter)
    return sessions
