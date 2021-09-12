import time
from datetime import datetime, timedelta
from abc import abstractmethod, ABCMeta

import requests

from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

PAGE_SIZE = 50000

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

BQ_CLIENT = bigquery.Client()
DATASET = "GoogleAnalytics"


class IReport(metaclass=ABCMeta):
    def __init__(self, model):
        self.view_id = model.view_id
        self.website = model.website
        self.principal_content_type = model.principal_content_type
        self.start = model.start
        self.end = model.end

        self.column_header = {}
        self.rows = []
        self.get_done = False
        self.next_page_token = None

    @property
    @abstractmethod
    def report(self):
        pass

    @property
    @abstractmethod
    def dimensions(self):
        pass

    @property
    @abstractmethod
    def metrics(self):
        pass

    @property
    def table(self):
        return f"{self.report}__{self.view_id}"

    def get_request(self):
        request = {
            "dateRanges": {
                "startDate": self.start,
                "endDate": self.end,
            },
            "viewId": self.view_id,
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
        if self.next_page_token:
            request["pageToken"] = self.next_page_token
        return request

    def transform(self):
        if self.rows:
            dimension_header = self.column_header["dimensions"]
            metric_header = self.column_header["metricHeader"]["metricHeaderEntries"]
            dimension_header = [
                (lambda x: x.replace("ga:", ""))(i) for i in dimension_header
            ]
            metric_header = [
                (lambda x: x["name"].replace("ga:", ""))(i) for i in metric_header
            ]

            rows = []
            for row in self.rows:
                dimension_values = dict(zip(dimension_header, row["dimensions"]))
                metric_values = dict(zip(metric_header, row["metrics"][0]["values"]))
                dimension_values["date"] = datetime.strptime(
                    dimension_values["date"], "%Y%m%d"
                ).strftime(DATE_FORMAT)
                rows.append(
                    {
                        **dimension_values,
                        **metric_values,
                        "_website": self.website,
                        "_principal_content_type": self.principal_content_type,
                        "_batched_at": NOW.isoformat(timespec="seconds"),
                    }
                )
            self.rows = rows
        else:
            self.output_rows = None

    def load(self):
        job = BQ_CLIENT.load_table_from_json(
            self.rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        )
        job.add_done_callback(self._load_callback)
        return job

    def _load_callback(self, job):
        self._update()
        self.output_rows = job.result().output_rows

    def _update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.dimensions),
            incre_key="_batched_at",
        )
        BQ_CLIENT.query(rendered_query)


class Demographics(IReport):
    report = "Demographics"
    dimensions = [
        "date",
        "channelGrouping",
        "deviceCategory",
        "userType",
        "country",
    ]
    metrics = [
        "users",
        "newUsers",
        "sessionsPerUser",
        "sessions",
        "pageviews",
        "pageviewsPerSession",
        "avgSessionDuration",
        "bounceRate",
    ]
    schema = [
        {"name": "date", "type": "DATE"},
        {"name": "deviceCategory", "type": "STRING"},
        {"name": "channelGrouping", "type": "STRING"},
        {"name": "userType", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "users", "type": "INTEGER"},
        {"name": "newUsers", "type": "INTEGER"},
        {"name": "sessionsPerUser", "type": "FLOAT"},
        {"name": "sessions", "type": "INTEGER"},
        {"name": "pageviews", "type": "INTEGER"},
        {"name": "pageviewsPerSession", "type": "FLOAT"},
        {"name": "avgSessionDuration", "type": "FLOAT"},
        {"name": "bounceRate", "type": "FLOAT"},
        {"name": "_website", "type": "STRING"},
        {"name": "_principal_content_type", "type": "STRING"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]


class Ages(IReport):
    report = "Ages"
    dimensions = [
        "date",
        "channelGrouping",
        "deviceCategory",
        "userAgeBracket",
    ]
    metrics = [
        "users",
        "newUsers",
        "sessionsPerUser",
        "sessions",
        "pageviews",
        "pageviewsPerSession",
        "avgSessionDuration",
        "bounceRate",
    ]
    schema = [
        {"name": "date", "type": "DATE"},
        {"name": "deviceCategory", "type": "STRING"},
        {"name": "channelGrouping", "type": "STRING"},
        {"name": "userAgeBracket", "type": "STRING"},
        {"name": "users", "type": "INTEGER"},
        {"name": "newUsers", "type": "INTEGER"},
        {"name": "sessionsPerUser", "type": "FLOAT"},
        {"name": "sessions", "type": "INTEGER"},
        {"name": "pageviews", "type": "INTEGER"},
        {"name": "pageviewsPerSession", "type": "FLOAT"},
        {"name": "avgSessionDuration", "type": "FLOAT"},
        {"name": "bounceRate", "type": "FLOAT"},
        {"name": "_website", "type": "STRING"},
        {"name": "_principal_content_type", "type": "STRING"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]


class Acquisitions(IReport):
    report = "Acquisitions"
    dimensions = [
        "date",
        "deviceCategory",
        "channelGrouping",
        "socialNetwork",
        "fullReferrer",
        "pagePath",
    ]
    metrics = [
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
    schema = [
        {"name": "date", "type": "STRING"},
        {"name": "deviceCategory", "type": "STRING"},
        {"name": "channelGrouping", "type": "STRING"},
        {"name": "socialNetwork", "type": "STRING"},
        {"name": "fullReferrer", "type": "STRING"},
        {"name": "pagePath", "type": "STRING"},
        {"name": "users", "type": "INTEGER"},
        {"name": "newUsers", "type": "INTEGER"},
        {"name": "sessions", "type": "INTEGER"},
        {"name": "pageviews", "type": "INTEGER"},
        {"name": "avgSessionDuration", "type": "FLOAT"},
        {"name": "bounceRate", "type": "FLOAT"},
        {"name": "avgTimeOnPage", "type": "FLOAT"},
        {"name": "totalEvents", "type": "INTEGER"},
        {"name": "uniqueEvents", "type": "INTEGER"},
        {"name": "_website", "type": "STRING"},
        {"name": "_principal_content_type", "type": "STRING"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]


class Events(IReport):
    report = "Events"
    dimensions = [
        "date",
        "deviceCategory",
        "channelGrouping",
        "eventCategory",
        "eventAction",
    ]
    metrics = [
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
    schema = [
        {"name": "date", "type": "STRING"},
        {"name": "deviceCategory", "type": "STRING"},
        {"name": "channelGrouping", "type": "STRING"},
        {"name": "eventCategory", "type": "STRING"},
        {"name": "eventAction", "type": "STRING"},
        {"name": "users", "type": "INTEGER"},
        {"name": "newUsers", "type": "INTEGER"},
        {"name": "sessions", "type": "INTEGER"},
        {"name": "pageviews", "type": "INTEGER"},
        {"name": "avgSessionDuration", "type": "FLOAT"},
        {"name": "bounceRate", "type": "FLOAT"},
        {"name": "avgTimeOnPage", "type": "FLOAT"},
        {"name": "totalEvents", "type": "INTEGER"},
        {"name": "uniqueEvents", "type": "INTEGER"},
        {"name": "_website", "type": "STRING"},
        {"name": "_principal_content_type", "type": "STRING"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]


class UAJob:
    def __init__(self, headers, view_id, website, principal_content_type, start, end):
        self.headers = headers
        self.view_id = view_id
        self.website = website
        self.principal_content_type = principal_content_type
        self.start, self.end = self._get_time_range(start, end)
        self.reports = [
            Demographics(self),
            Ages(self),
            Acquisitions(self),
            Events(self),
        ]

    def _get_time_range(self, _start, _end):
        if _start and _end:
            start, end = _start, _end
        else:
            end = NOW.strftime(DATE_FORMAT)
            start = (NOW - timedelta(days=3)).strftime(DATE_FORMAT)
        return start, end

    def _get(self):
        url = "https://analyticsreporting.googleapis.com/v4/reports:batchGet"
        with requests.Session() as session:
            while True:
                request_body = {
                    "reportRequests": [report.get_request() for report in self.reports],
                }
                with session.post(url, json=request_body, headers=self.headers) as r:
                    r.raise_for_status()
                    res = r.json()
                _reports = res["reports"]
                for report, report_res in zip(self.reports, _reports):
                    report.column_header = report_res["columnHeader"]
                    if report_res["data"].get("rows", []):
                        if not report.get_done:
                            report.rows.extend(report_res["data"]["rows"])
                        next_page_token = report_res.get("nextPageToken")
                        if next_page_token:
                            report.next_page_token = next_page_token
                        else:
                            report.get_done = True
                    else:
                        report.get_done = True
                if not [report for report in self.reports if report.get_done is False]:
                    break
        return sum([len(report.rows) for report in self.reports])

    def _transform(self):
        [report.transform() for report in self.reports]

    def _load(self):
        load_jobs = [report.load() for report in self.reports if report.rows]
        while [job for job in load_jobs if job.state not in ("DONE", "SUCCESS")]:
            time.sleep(5)

    def run(self):
        num_processed = self._get()
        response = {
            "view_id": self.view_id,
            "start": self.start,
            "end": self.end,
            "reports": [
                {
                    "report": report.report,
                    "num_processed": len(report.rows),
                }
                for report in self.reports
            ],
        }
        if num_processed > 0:
            self._transform()
            self._load()
            response["reports"] = [
                {
                    **report_res,
                    "output_rows": report.output_rows,
                }
                for report, report_res in zip(self.reports, response["reports"])
            ]
        return response
