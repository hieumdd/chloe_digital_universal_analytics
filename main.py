import os
import sys
import json
import itertools
from datetime import datetime, timedelta

import requests

from models import UAJobs

# if sys.platform == "win32":
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def main():
    job = UAJobs('214881178', '2021-01-01', '2021-02-01')
    response = job.run()
    return response, 200


main()
