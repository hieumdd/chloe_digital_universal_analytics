from .utils import process_broadcast

START = "2021-06-01"
END = "2021-06-14"


def test_broadcast_auto():
    data = {
        "broadcast": True,
    }
    process_broadcast(data)


def test_broadcast_manual():
    data = {
        "broadcast": True,
        "start": START,
        "end": END,
    }
    process_broadcast(data)
