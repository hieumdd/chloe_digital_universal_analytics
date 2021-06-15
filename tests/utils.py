import json
import base64

def encode_data(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    return {"message": {"data": data_encoded}}

def assertion(res):
    for i in res['results']:
        assert i["num_processed"] > 0
        assert i["output_rows"] > 0
        assert i["num_processed"] == i["output_rows"]
        assert i["errors"] is None
