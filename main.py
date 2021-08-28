import base64
import json

from models import UAJob
from broadcast import broadcast_email, broadcast_job


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        if 'refresh_token' in data:
            job = broadcast_job
        else:
            job = broadcast_email
        results = job(data)
    elif "view_id" in data:
        job = UAJob(
            email=data["email"],
            account=data["account"],
            property=data["property"],
            view=data["view"],
            view_id=data["view_id"],
            headers=data["headers"],
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
