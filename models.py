import os
import json
import time
from datetime import datetime, timedelta
from abc import abstractmethod, ABCMeta

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
PAGE_SIZE = 50000

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

BQ_CLIENT = bigquery.Client()
DATASET = "GoogleAnalytics"


class IReport(metaclass=ABCMeta):
    def __init__(self, email, account, property, view, view_id, start, end):
        self.email = email
        self.account = account
        self.property = property
        self.view = view
        self.view_id = view_id
        self.start = start
        self.end = end

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

    @property
    def schema(self):
        with open(f"schemas/{self.report}.json", "r") as f:
            return json.load(f)

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
                    "_email": self.email,
                    "_account": self.account,
                    "_property": self.property,
                    "_view": self.view,
                    "_batched_at": NOW.isoformat(timespec="seconds"),
                }
            )
        self.rows = rows

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
        self.loads = job.result()

    def _update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.dimensions),
            incremental_key="_batched_at",
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


class UAJob:
    def __init__(self, email, account, property, view, view_id, start, end):
        self.email = email
        self.account = account
        self.property = property
        self.view = view
        self.view_id = view_id
        self.start, self.end = self._get_time_range(start, end)
        self.reports = [
            i(email, account, property, view, view_id, self.start, self.end)
            for i in [
                Demographics,
                Ages,
                Acquisitions,
                Events,
            ]
        ]

    def _get_time_range(self, _start, _end):
        if _start and _end:
            start, end = _start, _end
        else:
            end = NOW.strftime(DATE_FORMAT)
            start = (NOW - timedelta(days=3)).strftime(DATE_FORMAT)
        return start, end

    def _get(self):
        # credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        #     json.loads(os.getenv("GCP_SA_KEY")),
        #     scopes=scopes,
        # )
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            scopes=SCOPES,
        )
        service = build("analyticsreporting", "v4", credentials=credentials)
        while True:
            request_body = {
                "reportRequests": [report.get_request() for report in self.reports],
            }
            res = service.reports().batchGet(body=request_body).execute()
            _reports = res["reports"]
            for report, report_res in zip(self.reports, _reports):
                report.column_header = report_res["columnHeader"]
                if not report.get_done:
                    report.rows.extend(report_res["data"]["rows"])
                next_page_token = report_res.get("nextPageToken")
                if next_page_token:
                    report.next_page_token = next_page_token
                else:
                    report.get_done = True
            if not [report for report in self.reports if report.get_done is False]:
                break
        return sum([len(report.rows) for report in self.reports])

    def _transform(self):
        [report.transform() for report in self.reports]

    def _load(self):
        load_jobs = [report.load() for report in self.reports]
        while [job for job in load_jobs if job.state != "DONE"]:
            time.sleep(5)
        return [report.loads for report in self.reports]

    def run(self):
        num_processed = self._get()
        response = {
            "email": self.email,
            "account": self.account,
            "property": self.property,
            "view": self.view,
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
                    "output_rows": report.loads.output_rows,
                }
                for report, report_res in zip(self.reports, response["reports"])
            ]
        return response
