import json
import base64
from unittest.mock import Mock

import pytest

from main import main
from broadcast import get_token
from .utils import process

VIEW_ID = "63797302"
EMAIL = "metrics@"
HEADERS = get_token(EMAIL)

ID = {
    "view_id": "63797302",
    "headers": HEADERS,
}

START = "2021-09-01"
END = "2021-09-02"

DATE = {
    "start": START,
    "end": END,
}

def run(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    message = {
        "message": {
            "data": data_encoded,
        },
    }
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    return res


@pytest.mark.parametrize(
    "data",
    [
        ID,
        {**ID, **DATE},
    ],
    ids=["auto", "manual"],
)
def test_units(data):
    res = run(data)
    results = res['results']
    for i in results['reports']:
        assert i["num_processed"] >= 0
        if i["num_processed"] > 0:
            assert i["output_rows"] == i["num_processed"]
