import base64
import json

from google.cloud import bigquery

from models import UAJobs
from broadcaster import broadcast

BQ_CLIENT = bigquery.Client()


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if data:
        if "broadcast" in data:
            job = broadcast()
        if "view_id" in data:
            job = UAJobs(
                bq_client=BQ_CLIENT,
                accounts=data["accounts"],
                properties=data["properties"],
                views=data["views"],
                view_id=data["view_id"],
                headers=data.get("headers"),
                start=data.get("start"),
                end=data.get("end"),
            )
    response = job.run()
    print(response)
    return response
