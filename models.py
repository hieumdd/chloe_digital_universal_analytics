import os
import json
from datetime import datetime, timedelta
from urllib3.util.retry import Retry
from abc import abstractmethod, ABCMeta

import requests
from requests.adapters import HTTPAdapter
from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

PAGE_SIZE = 50000

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

BQ_CLIENT = bigquery.Client()
DATASET = "GoogleAnalytics"


def get_headers():
    """Create headers from Credentials

    Returns:
        dict: HTTP Headers
    """

    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }
    with requests.post(
        "https://oauth2.googleapis.com/token",
        params=params,
    ) as r:
        access_token = r.json()["access_token"]
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def get_sessions():
    sessions = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 503, 500],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sessions.mount("https://", adapter)
    return sessions


class UAReports(metaclass=ABCMeta):
    @property
    @abstractmethod
    def report(self):
        """Report Name"""
        pass

    @property
    @abstractmethod
    def dimensions(self):
        """Dimensions"""
        pass

    @property
    @abstractmethod
    def metrics(self):
        """Metrics"""
        pass

    @property
    @abstractmethod
    def table(self):
        return f"{self.report}__{self.properties}__{self.views}"

    @property
    def schema(self):
        """Get schema

        Returns:
            dict: Schema
        """

        with open(f"schemas/{self.report}.json", "r") as f:
            return json.load(f)

    def __init__(self, sessions, headers, options, start, end):
        self.sessions = sessions
        self.headers = headers
        self.options = options
        self.start = start
        self.end = end

    def _get(self):
        """Get the data from API

        Returns:
            tuple: (column_header, rows))
        """

        rows = []
        payload = {
            "reportRequests": [
                {
                    "dateRanges": {
                        "startDate": self.start,
                        "endDate": self.end,
                    },
                    "viewId": self.options["view_id"],
                    "dimensions": [
                        {
                            "name": f"ga:{dimension}",
                        }
                        for dimension in self.dimensions
                    ],
                    "metrics": [
                        {
                            "expression": f"ga:{metric}",
                        }
                        for metric in self.metrics
                    ],
                    "pageSize": PAGE_SIZE,
                }
            ],
        }
        url = "https://analyticsreporting.googleapis.com/v4/reports:batchGet"
        while True:
            with self.sessions.post(
                url,
                headers=self.headers,
                json=payload,
            ) as r:
                res = r.json()
            report = res["reports"][0]
            column_header = report["columnHeader"]
            data = report["data"]
            _rows = data.get("rows")
            if _rows:
                rows.extend(_rows)
                next_page_token = data.get("nextPageToken", None)
                if next_page_token:
                    payload["reportRequests"][0]["pageToken"] = next_page_token
                else:
                    break
            else:
                rows = []
        return column_header, rows

    def _transform(self, column_headers, _rows):
        """Transform/parse the results

        Args:
            column_headers (list): Column Headers
            _rows (list): List of results

        Returns:
            list: List of results
        """

        dimension_headers = column_headers["dimensions"]
        metric_headers = column_headers["metricHeader"]["metricHeaderEntries"]
        dimension_headers = [
            (lambda x: x.replace("ga:", ""))(i) for i in dimension_headers
        ]
        metric_headers = [
            (lambda x: x["name"].replace("ga:", ""))(i) for i in metric_headers
        ]

        rows = []
        for row in _rows:
            dimension_values = dict(zip(dimension_headers, row["dimensions"]))
            metric_values = dict(zip(metric_headers, row["metrics"][0]["values"]))
            dimension_values["date"] = datetime.strptime(
                dimension_values["date"], "%Y%m%d"
            ).strftime(DATE_FORMAT)
            rows.append(
                {
                    **dimension_values,
                    **metric_values,
                    "_batched_at": NOW.isoformat(timespec="seconds"),
                }
            )
        return rows

    def _load(self, rows):
        """Load to BigQuery

        Args:
            rows (list): Lit of results

        Returns:
            google.cloud.bigquery.job.LoadJob: Load Job result
        """

        BQ_CLIENT.create_dataset(self.options["accounts"], exists_ok=True)
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

    def _update(self):
        """Update from stage table to main table"""

        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.dimensions),
            incremental_key="_batched_at",
        )
        BQ_CLIENT.query(rendered_query)

    def run(self):
        """Main run function

        Returns:
            dict: Job responses
        """

        column_headers, rows = self._get()
        responses = {
            "report": self.report,
            "options": self.options,
            "start": self.start,
            "end": self.end,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self._transform(column_headers, rows)
            loads = self._load(rows)
            self._update()
            responses["output_rows"] = loads.output_rows

        return responses


class Demographics(UAReports):
    def __init__(self, sessions, headers, options, start, end):
        super().__init__(sessions, headers, options, start, end)

    @property
    def report(self):
        return "Demographics"

    @property
    def dimensions(self):
        return [
            "date",
            "channelGrouping",
            "deviceCategory",
            "userType",
            "country",
        ]

    @property
    def metrics(self):
        return [
            "users",
            "newUsers",
            "sessionsPerUser",
            "sessions",
            "pageviews",
            "pageviewsPerSession",
            "avgSessionDuration",
            "bounceRate",
        ]


class Ages(UAReports):
    def __init__(self, sessions, headers, options, start, end):
        super().__init__(sessions, headers, options, start, end)

    @property
    def report(self):
        return "Ages"

    @property
    def dimensions(self):
        return [
            "date",
            "channelGrouping",
            "deviceCategory",
            "userAgeBracket",
        ]

    @property
    def metrics(self):
        return [
            "users",
            "newUsers",
            "sessionsPerUser",
            "sessions",
            "pageviews",
            "pageviewsPerSession",
            "avgSessionDuration",
            "bounceRate",
        ]


class Acquisitions(UAReports):
    def __init__(self, sessions, headers, options, start, end):
        super().__init__(sessions, headers, options, start, end)

    @property
    def report(self):
        return "Acquisitions"

    @property
    def dimensions(self):
        return [
            "date",
            "deviceCategory",
            "channelGrouping",
            "socialNetwork",
            "fullReferrer",
            "pagePath",
        ]

    @property
    def metrics(self):
        return [
            "users",
            "newUsers",
            "sessions",
            "pageviews",
            "avgSessionDuration",
            "bounceRate",
            "avgTimeOnPage",
            "totalEvents",
            "uniqueEvents",
        ]


class Events(UAReports):
    def __init__(self, sessions, headers, options, start, end):
        super().__init__(sessions, headers, options, start, end)

    @property
    def report(self):
        return "Events"

    @property
    def dimensions(self):
        return [
            "date",
            "deviceCategory",
            "channelGrouping",
            "eventCategory",
            "eventAction",
        ]

    @property
    def metrics(self):
        return [
            "users",
            "newUsers",
            "sessions",
            "pageviews",
            "avgSessionDuration",
            "bounceRate",
            "avgTimeOnPage",
            "totalEvents",
            "uniqueEvents",
        ]


class UAJobs:
    def __init__(self, options, start, end, headers=None):
        self.options = options
        self.start, self.end = self._get_time_range(start, end)
        self.sessions = get_sessions()
        if not headers:
            self.headers = get_headers()
        else:
            self.headers = headers

    

    def _get_time_range(self, start, end):
        """Set the time range

        Args:
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d

        Returns:
            tuple: (start, end)
        """

        if start and end:
            return start, end
        else:
            end = NOW.strftime(DATE_FORMAT)
            start = (NOW - timedelta(days=10)).strftime(DATE_FORMAT)
            return start, end

    def run(self):
        """Create reports to run

        Returns:
            dict: Run responses
        """

        reports = [
            i(
                self.sessions,
                self.headers,
                self.options,
                self.start,
                self.end,
            )
            for i in [
                Demographics,
                Ages,
                Acquisitions,
                Events,
            ]
        ]
        return [i.run() for i in reports]
