import base64
import json

from models import UAJob
from broadcast import broadcast


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        results = broadcast(
            data.get("start"),
            data.get("end"),
        )
    elif "view_id" in data:
        job = UAJob(
            options=data["options"],
            headers=data.get("headers"),
            start=data.get("start"),
            end=data.get("end"),
        )
        results = job.run()
    else:
        raise NotImplementedError(data)

    response = {
        "pipelines": "GA",
        "results": results,
    }
    print(response)
    return response
