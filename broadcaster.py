import os
import json

import google.auth
from google.cloud import pubsub_v1, bigquery
import jinja2

from models import create_headers

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


def broadcast(start=None, end=None):
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    bq_client = bigquery.Client(credentials=credentials, project=project)

    view_ids = get_view_ids(bq_client)
    # headers = create_headers()
    # publisher = pubsub_v1.PublisherClient()
    # topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))
    # for i in view_ids:
    #     data = {**i, **{"headers": headers}, **{"start": start, "end": end}}
    #     message_json = json.dumps(data)
    #     message_bytes = message_json.encode("utf-8")
    #     publisher.publish(topic_path, data=message_bytes).result()
    create_union(bq_client, view_ids)
    return {"message_sent": len(view_ids)}


def get_view_ids(bq_client):
    rows = bq_client.query("SELECT * FROM config._ext_UAViews").result()
    return [dict(row.items()) for row in rows]


def create_union(bq_client, view_ids):
    template = TEMPLATE_ENV.get_template("create_union_all.sql.j2")
    schema_path = "schemas/"
    for i in os.listdir(schema_path):
        with open(schema_path + i, "r") as f:
            schema = json.load(f)
        fields = [i["name"] for i in schema]
        rendered_query = template.render(
            project_id=os.getenv("PROJECT_ID"),
            view_ids=view_ids,
            fields=fields,
            report_name=i.replace(".json", ""),
        )
        _ = bq_client.query(rendered_query).result()
