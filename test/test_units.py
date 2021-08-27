from .utils import process

OPTIONS = {
    "accounts": "AshleyAndEmily",
    "properties": "AshleyAndEmily",
    "views": "AllWebSiteData",
    "view_id": "87741998",
}
START = "2021-06-01"
END = "2021-06-14"


def test_single_auto():
    data = {
        "options": OPTIONS,
    }
    process(data)


def test_single_manual():
    data = {
        "options": OPTIONS,
        "start": START,
        "end": END,
    }
    process(data)
