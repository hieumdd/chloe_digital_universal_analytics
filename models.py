import os
import json
from datetime import datetime, timedelta
from abc import abstractmethod, ABCMeta

import requests
from google.cloud import bigquery
import jinja2

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


def create_headers():
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
    with requests.post("https://oauth2.googleapis.com/token", params=params) as r:
        access_token = r.json().get("access_token")
    return {
        "Authorization": "Bearer " + access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


class UAReports(metaclass=ABCMeta):
    def __init__(
        self,
        sessions,
        bq_client,
        headers,
        accounts,
        properties,
        views,
        view_id,
        start,
        end,
    ):
        """Initialize report run

        Args:
            sessions (requests.Session): HTTP Client
            bq_client (google.cloud.bigquery.Client): BQ Client
            headers (dict): HTTP Headers
            accounts (str): Account Name
            properties (str): Property Name
            views (str): View Name
            view_id (str): View ID
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d
        """

        self.sessions = sessions
        self.bq_client = bq_client
        self.headers = headers
        self.accounts = accounts
        self.properties = properties
        self.views = views
        self.view_id = view_id
        self.start = start
        self.end = end
        self.dimensions = self.get_dimensions()
        self.metrics = self.get_metrics()
        self.job_ts = datetime.now().isoformat()

    @staticmethod
    def create(
        sessions,
        bq_client,
        headers,
        accounts,
        properties,
        views,
        view_id,
        mode,
        start=None,
        end=None,
    ):
        """Factory method to create report

        Args:
            sessions (requests.Session): HTTP Client
            bq_client (google.cloud.bigquery.Client): BQ Client
            headers (dict): HTTP Headers
            accounts (str): Account Name
            properties (str): Property Name
            views (str): View Name
            view_id (str): View ID
            mode (str): Mode
            start (str, optional): Date in %Y-%m-%d. Defaults to None.
            end (str, optional): Date in %Y-%m-%d. Defaults to None.

        Raises:
            NotImplementedError: Not found

        Returns:
            UAReport: Report
        """

        mapper = {
            'demographics': DemographicsReport,
            'ages': AgesReport,
            'acquisitions': AcquisitionsReport,
            'events': EventsReport
        }
        if mode in mapper:
            return mapper[mode](sessions,
                bq_client,
                headers,
                accounts,
                properties,
                views,
                view_id,
                start,
                end)
        else:
            raise NotImplementedError

    @abstractmethod
    def get_dimensions(self):
        """Get dimensions"""
        pass

    @abstractmethod
    def get_metrics(self):
        """Get metrics"""
        pass

    def make_requests(self):
        """Make report request body

        Returns:
            dict: Payload
        """

        return {
            "dateRanges": {
                "startDate": self.start,
                "endDate": self.end,
            },
            "viewId": self.view_id,
            "dimensions": [
                {"name": f"ga:{dimension}"} for dimension in self.dimensions
            ],
            "metrics": [{"expression": f"ga:{metric}"} for metric in self.metrics],
            "pageSize": "50000",
        }

    def fetch(self):
        """Get the data from API

        Returns:
            tuple: (column_header, rows))
        """

        rows = []
        payload = {"reportRequests": [self.make_requests()]}
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
            _rows = data.get('rows')
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

    def transform(self, column_headers, _rows):
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
            ).strftime("%Y-%m-%d")
            rows.append(
                {**dimension_values, **metric_values, **{"_batched_at": self.job_ts}}
            )
        self.num_processed = len(rows)
        return rows

    def load(self, rows):
        """Load to BigQuery

        Args:
            rows (list): Lit of results

        Returns:
            google.cloud.bigquery.job.LoadJob: Load Job result
        """

        dataset = self.create_dataset()
        table = self.get_table()
        schema = self.get_schema()
        return self.bq_client.load_table_from_json(
            rows,
            f"{dataset}._stage_{table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
            ),
        ).result()

    def create_dataset(self):
        """Create dataset if not exists

        Returns:
            str: Account Name
        """

        self.bq_client.create_dataset(self.accounts, exists_ok=True)
        return self.accounts

    @abstractmethod
    def get_table(self):
        """Get table name"""
        pass

    def get_schema(self):
        """Get schema

        Returns:
            dict: Schema
        """

        report_name = self.get_report_name()
        with open(f'schemas/{report_name}.json', 'r') as f:
            schema = json.load(f)
        return schema

    def update(self):
        """Update from stage table to main table"""

        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=self.accounts,
            table=self.get_table(),
            p_key=",".join(self.get_dimensions()),
            incremental_key="_batched_at",
        )

        self.bq_client.query(rendered_query)

    def run(self):
        """Main run function

        Returns:
            dict: Job responses
        """

        column_headers, rows = self.fetch()
        if len(rows) > 0:
            rows = self.transform(column_headers, rows)
            results = self.load(rows)
            self.update()
            run_responses = {
                "num_processed": self.num_processed,
                "output_rows": getattr(results, "output_rows", None),
                "errors": getattr(results, "errors", None),
            }
        else:
            run_responses = {
                "status": "no rows"
            }

        return {
            "accounts": self.accounts,
            "properties": self.properties,
            "views": self.views,
            "view_id": self.view_id,
            "report": self.get_report_name(),
            "start": self.start,
            "end": self.end,
            **run_responses
        }

    @abstractmethod
    def get_report_name(self):
        """Get report name"""
        pass


class DemographicsReport(UAReports):
    def __init__(
        self,
        sessions,
        bq_client,
        headers,
        accounts,
        properties,
        views,
        view_id,
        start,
        end,
    ):
        super().__init__(
            sessions,
            bq_client,
            headers,
            accounts,
            properties,
            views,
            view_id,
            start,
            end,
        )

    def get_dimensions(self):
        return ["date", "channelGrouping", "deviceCategory", "userType", "country"]

    def get_metrics(self):
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

    def get_table(self):
        return f"{self.properties}__{self.views}__DemographicsReport"

    def get_report_name(self):
        return "Demographics"

class AgesReport(UAReports):
    def __init__(
        self,
        sessions,
        bq_client,
        headers,
        accounts,
        properties,
        views,
        view_id,
        start,
        end,
    ):
        super().__init__(
            sessions,
            bq_client,
            headers,
            accounts,
            properties,
            views,
            view_id,
            start,
            end,
        )

    def get_dimensions(self):
        return ["date", "channelGrouping", "deviceCategory", "userAgeBracket"]

    def get_metrics(self):
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

    def get_table(self):
        return f"{self.properties}__{self.views}__AgesReport"

    def get_report_name(self):
        return "Ages"


class AcquisitionsReport(UAReports):
    def __init__(
        self,
        sessions,
        bq_client,
        headers,
        accounts,
        properties,
        views,
        view_id,
        start,
        end,
    ):
        super().__init__(
            sessions,
            bq_client,
            headers,
            accounts,
            properties,
            views,
            view_id,
            start,
            end,
        )

    def get_dimensions(self):
        return [
            "date",
            "deviceCategory",
            "channelGrouping",
            "socialNetwork",
            "fullReferrer",
            "pagePath"
        ]

    def get_metrics(self):
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

    def get_table(self):
        return f"{self.properties}__{self.views}__AcquisitionsReport"

    def get_report_name(self):
        return "Acquisitions"


class EventsReport(UAReports):
    def __init__(
        self,
        sessions,
        bq_client,
        headers,
        accounts,
        properties,
        views,
        view_id,
        start,
        end,
    ):
        super().__init__(
            sessions,
            bq_client,
            headers,
            accounts,
            properties,
            views,
            view_id,
            start,
            end,
        )

    def get_dimensions(self):
        return [
            "date",
            "deviceCategory",
            "channelGrouping",
            "eventCategory",
            "eventAction",
        ]

    def get_metrics(self):
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

    def get_table(self):
        return f"{self.properties}__{self.views}__EventsReport"

    def get_report_name(self):
        return "Events"


class UAJobs:
    def __init__(
        self,
        bq_client,
        accounts,
        properties,
        views,
        view_id,
        headers=None,
        start=None,
        end=None,
    ):
        """Create report runs

        Args:
            bq_client (google.cloud.bigquery.Client): BQ Client
            accounts (str): Account Name
            properties (str): Property Name
            views (str): View Name
            view_id (str): View ID
            headers (dict, optional): HTTP Headers. Defaults to None.
            start (str, optional): Date in %Y-%m-%d. Defaults to None.
            end (str, optional): Date in %Y-%m-%d. Defaults to None.
        """

        self.bq_client = bq_client
        self.accounts = accounts
        self.properties = properties
        self.views = views
        self.view_id = view_id
        self.start, self.end = self.get_time_range(start, end)
        self.sessions = requests.Session()
        if not headers:
            self.headers = create_headers()
        else:
            self.headers = headers

    def get_time_range(self, start, end):
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
            now = datetime.now()
            end = now.strftime("%Y-%m-%d")
            start = (now - timedelta(days=10)).strftime("%Y-%m-%d")
            return start, end

    def run(self):
        """Create reports to run

        Returns:
            dict: Run responses
        """
                
        reports = [
            UAReports.create(
                self.sessions,
                self.bq_client,
                self.headers,
                self.accounts,
                self.properties,
                self.views,
                self.view_id,
                i,
                self.start,
                self.end,
            )
            for i in ["demographics", "ages", "acquisitions", "events"]
        ]
        response = {
            "pipelines": "Universal Analytics",
            "results": [i.run() for i in reports],
        }
        return response
