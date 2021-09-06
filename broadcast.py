import os
import json
import itertools

import requests
from google.cloud import pubsub_v1, secretmanager


BASE_ID = "apporLbA6XsKHTKpz"
VIEW = "Sorted by GA"


PUBLISHER_CLIENT = pubsub_v1.PublisherClient()
TOPIC_PATH = PUBLISHER_CLIENT.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))

SECRET_CLIENT = secretmanager.SecretManagerServiceClient()
SECRET_MAP = [
    {
        "email": "metrics@",
        "secret": ("ga_metrics_refresh_token", 1),
    },
]


def get_accounts():
    url = f"https://api.airtable.com/v0/{BASE_ID}/CLIENT%20DETAILS"
    params = {
        "view": VIEW,
        "fields%5B%5D": [
            "Website",
            "GA account",
            "Principle Content Type",
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
            "view_id": row["fields"].get("view_id"),
            "principal_content_type": row["fields"].get("Principal Content Type"),
        }
        for row in rows
        if row["fields"].get("GA account")
    ]
    key = lambda x: (x["email"])
    rows_groupby = [
        {
            "key": k,
            "value": [i for i in v],
        }
        for k, v in itertools.groupby(sorted(rows, key=key), key)
    ]
    rows_groupby = [i for i in rows_groupby if i["key"] == "metrics@"]
    return rows_groupby


def get_token(email):
    secret_id, version_id = [i["secret"] for i in SECRET_MAP if i["email"] == email][0]
    name = (
        f"projects/{os.getenv('PROJECT_ID')}/secrets/{secret_id}/versions/{version_id}"
    )
    response = SECRET_CLIENT.access_secret_version(request={"name": name})
    refresh_token = response.payload.data.decode("UTF-8")
    params = {
        "client_id": os.getenv("CLIENT_ID"),
        "client_secret": os.getenv("CLIENT_SECRET"),
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    with requests.post("https://oauth2.googleapis.com/token", params=params) as r:
        access_token = r.json()["access_token"]
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def broadcast(broadcast_data):
    accounts = get_accounts()
    for account in accounts:
        headers = get_token(account["key"])
        for view in account["value"]:
            data = {
                "headers": headers,
                "view_id": view["view_id"],
                "start": broadcast_data.get("start"),
                "end": broadcast_data.get("end"),
            }
            message_json = json.dumps(data)
            message_bytes = message_json.encode("utf-8")
            # PUBLISHER_CLIENT.publish(TOPIC_PATH, data=message_bytes).result()
    return {
        "broadcast": "job",
        "message_sent": len([i for i in accounts["value"]]),
    }


# x = broadcast({})
# x
