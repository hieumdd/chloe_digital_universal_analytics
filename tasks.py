import os
import json
import itertools
import uuid

import requests
from google.cloud import tasks_v2, secretmanager


BASE_ID = "apporLbA6XsKHTKpz"
VIEW = "Sorted by GA"

SECRET_CLIENT = secretmanager.SecretManagerServiceClient()
SECRET_MAP = [
    {
        "email": "metrics@",
        "secret": ("ga_metrics_refresh_token", 1),
    },
    {
        "email": "analytics@",
        "secret": ("ga_analytics_refresh_token", 1),
    },
    {
        "email": "cdbabe@",
        "secret": ("ga_cdbabe_refresh_token", 1),
    },
    {
        "email": "ga@",
        "secret": ("ga_ga_refresh_token", 1),
    },
    {
        "email": "hello@",
        "secret": ("ga_hello_refresh_token", 1),
    },
    {
        "email": "info@",
        "secret": ("ga_info_refresh_token", 1),
    },
    {
        "email": "poweredby@",
        "secret": ("ga_poweredby_refresh_token", 1),
    },
    {
        "email": "support@",
        "secret": ("ga_support_refresh_token", 1),
    },
]

TASKS_CLIENT = tasks_v2.CloudTasksClient()
CLOUD_TASKS_PATH = (
    os.getenv("PROJECT_ID"),
    os.getenv("REGION"),
    os.getenv("QUEUE_ID"),
)
PARENT = TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH)


def get_accounts():
    """Get accounts list from Airtable

    Returns:
        list: List of accounts
    """

    url = f"https://api.airtable.com/v0/{BASE_ID}/CLIENT%20DETAILS"
    params = {
        "view": VIEW,
        "fields%5B%5D": [
            "Website",
            "GA account",
            "Principle Content Type",
            "GA ID",
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
            "view_id": row["fields"].get("GA ID"),
            "active": row["fields"].get("Membership Status"),
            "principal_content_type": row["fields"].get("Principal Content Type"),
        }
        for row in rows
        if row["fields"].get("GA account")
        and row["fields"].get("Membership Status") == "Active"
    ]
    key = lambda x: (x["email"])
    rows_groupby = [
        {
            "key": k,
            "value": [i for i in v],
        }
        for k, v in itertools.groupby(sorted(rows, key=key), key)
    ]
    return rows_groupby


def get_token(email):
    """Get accounts' tokens from Secret Manager

    Args:
        email (str): Account's email

    Returns:
        dict: HTTP Headers
    """

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


def create_tasks(tasks_data):
    """Create tasks and put into queue

    Args:
        tasks_data (dict): Task request

    Returns:
        dict: Job Response
    """

    accounts = get_accounts()
    accounts_headers = [
        {
            **account,
            "headers": get_token(account["key"]),
        }
        for account in accounts
    ]
    payloads = [
        {
            "name": f"{view['view_id']}-{uuid.uuid4()}",
            "payload": {
                "headers": account["headers"],
                "view_id": view["view_id"],
                "website": view["website"],
                "principal_content_type": view["principal_content_type"],
                "start": tasks_data.get("start"),
                "end": tasks_data.get("end"),
            },
        }
        for account in accounts_headers
        for view in account["value"]
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(*CLOUD_TASKS_PATH, task=payload["name"]),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"https://{os.getenv('REGION')}-{os.getenv('PROJECT_ID')}.cloudfunctions.net/{os.getenv('FUNCTION_NAME')}",
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload["payload"]).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": PARENT,
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "messages_sent": len(responses),
        "tasks_data": tasks_data,
    }
