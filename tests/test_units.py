from unittest.mock import Mock

from main import main
from .utils import assertion, encode_data


def test_single_auto():
    data = {
        "accounts": "AshleyAndEmily",
        "properties": "AshleyAndEmily",
        "views": "AllWebSiteData",
        "view_id": "87741998",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_single_manual():
    data = {
        "accounts": "AshleyAndEmily",
        "properties": "AshleyAndEmily",
        "views": "AllWebSiteData",
        "view_id": "87741998",
        "start": "2021-06-01",
        "end": "2021-06-14",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_broadcast_auto():
    data = {"broadcast": True}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)


def test_broadcast_manual():
    data = {"broadcast": True, "start": "2021-06-01", "end": "2021-06-15"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)
