import os
import json
import itertools

import requests
from google.cloud import pubsub_v1
import jinja2

from models import get_headers

BASE_ID = "apporLbA6XsKHTKpz"

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

PUBLISHER = pubsub_v1.PublisherClient()
TOPIC_PATH = PUBLISHER.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))


def get_accounts():
    url = f"https://api.airtable.com/v0/{BASE_ID}/CLIENT%20DETAILS"
    params = {
        "view": "Active By Tier",
        "fields%5B%5D": [
            "Website",
            "GA account",
        ],
    }
    rows = []
    with requests.Session() as sessions:
        while True:
            with sessions.get(
                url,
                params=params,
                headers={
                    "Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}",
                },
            ) as r:
                res = r.json()
            rows.extend(res["records"])
            offset = res.get("offset")
            if offset:
                params["offset"] = offset
            else:
                break
    rows = [
        {
            "website": row["fields"].get("Website"),
            "email": row["fields"].get("GA account"),
            "refresh_token": "refresh_token",
        }
        for row in rows
        if row["fields"].get("GA account")
    ]
    key = lambda x: (x["email"], x["refresh_token"])
    rows_groupby = [
        {
            "key": k,
            "value": [i for i in v],
        }
        for k, v in itertools.groupby(sorted(rows, key=key), key)
    ]
    return rows_groupby


def publish(data):
    message_json = json.dumps(data)
    message_bytes = message_json.encode("utf-8")
    PUBLISHER.publish(TOPIC_PATH, data=message_bytes).result()


def broadcast_job(broadcast_data):
    headers = get_headers(broadcast_data["refresh_token"])
    value = broadcast_data["value"]
    for job in value:
        data = {
            "headers": headers,
            "email": job["email"],
            "account": job["account"],
            "property": job["property"],
            "view": job["view"],
            "view_id": job["view_id"],
            "start": broadcast_data.get("start"),
            "end": broadcast_data.get("end"),
        }
        publish(data)
    return {
        "broadcast": "job",
        "email": broadcast_data["key"]["email"],
        "message_sent": len(value),
    }


def broadcast_email(broadcast_data):
    accounts = get_accounts()
    for account in accounts:
        data = {
            "email": account["key"][0],
            "refresh_token": account["key"][1],
            "value": account["value"],
            "start": broadcast_data.get("start"),
            "end": broadcast_data.get("end"),
        }
        data
        # publish(data)
    return {
        "broadcast": "email",
        "message_sent": len(accounts),
    }

broadcast_email({})
