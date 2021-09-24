from models import UAJob
from tasks import create_tasks


def main(request):
    """API Gateway

    Args:
        request (flask.Request): HTTP request

    Raises:
        NotImplementedError: No module found

    Returns:
        dict: HTTP Response
    """

    data = request.get_json()
    print(data)

    if "tasks" in data:
        response = create_tasks(data)
    elif "view_id" in data and "broadcast" not in data:
        response = UAJob(
            headers=data["headers"],
            view_id=data["view_id"],
            website=data["website"],
            principal_content_type=data["principal_content_type"],
            start=data.get("start"),
            end=data.get("end"),
        ).run()
    else:
        raise NotImplementedError(data)

    print(response)
    return response
