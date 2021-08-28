import os

from models import get_headers
from .utils import process

EMAIL = ""
ACCOUNT = "AshleyAndEmily"
PROPERTY = "AshleyAndEmily"
VIEW = "AllWebSiteData"
VIEW_ID = "87741998"
HEADERS = get_headers(os.getenv('REFRESH_TOKEN'))


START = "2021-06-01"
END = "2021-06-14"


def test_single_auto():
    data = {
        "email": EMAIL,
        "account": ACCOUNT,
        "view": VIEW,
        "view_id": VIEW_ID,
        "headers": HEADERS,
    }
    process(data)


def test_single_manual():
    data = {
        "email": EMAIL,
        "account": ACCOUNT,
        "view": VIEW,
        "view_id": VIEW_ID,
        "refresh_token": REFRESH_TOKEN,
        "start": START,
        "end": END,
    }
    process(data)
