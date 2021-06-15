import os
import json

import google.auth
from google.cloud import pubsub_v1, bigquery

from models import create_headers


def broadcast():
    view_ids = get_view_ids()
    headers = create_headers()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))
    for i in view_ids:
        data = {**i, **{"headers": headers}}
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()
    return {"message_sent": len(view_ids)}


def get_view_ids():
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    bq_client = bigquery.Client(credentials=credentials, project=project)
    rows = bq_client.query("SELECT * FROM config._ext_UAViews").result()
    return [dict(row.items()) for row in rows]
