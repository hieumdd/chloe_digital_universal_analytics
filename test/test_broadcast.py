import os
from .utils import process_broadcast

EMAIL = ""
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')
VALUE = []

START = "2021-06-01"
END = "2021-06-14"


def test_broadcast_email_auto():
    data = {
        "broadcast": "email",
    }
    process_broadcast(data)


def test_broadcast_email_manual():
    data = {
        "broadcast": "email",
        "start": START,
        "end": END,
    }
    process_broadcast(data)


def test_broadcast_job_auto():
    data = {
        "email": EMAIL,
        "refresh_token": REFRESH_TOKEN,
        "value": VALUE,
        "start": START,
        "end": END,
    }
    process_broadcast(data)


def test_broadcast_job_manual():
    data = {
        "email": EMAIL,
        "refresh_token": REFRESH_TOKEN,
        "value": VALUE,
    }
    process_broadcast(data)
