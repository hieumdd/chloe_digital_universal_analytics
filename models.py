import os
import sys
import json
import itertools
from datetime import datetime, timedelta
from abc import abstractmethod, ABCMeta

import requests

# if sys.platform == "win32":
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")


class UAReports(metaclass=ABCMeta):
    def __init__(self, ga_view_id, start, end):
        self.ga_view_id = ga_view_id
        self.start = start
        self.end = end
        self.dimensions = self.get_dimensions()
        self.metrics = self.get_metrics()

    @staticmethod
    def create(ga_view_id, mode, start, end):
        if mode == "demographics":
            return DemographicsReport(ga_view_id, start, end)
        elif mode == "acquisitions":
            return AcquisitionsReport(ga_view_id, start, end)
        else:
            raise NotImplementedError

    @abstractmethod
    def get_dimensions(self):
        raise NotImplementedError

    @abstractmethod
    def get_metrics(self):
        raise NotImplementedError

    def create_header(self):
        params = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
            "grant_type": "refresh_token",
        }
        with self.sessions.post(
            "https://oauth2.googleapis.com/token", params=params
        ) as r:
            access_token = r.json().get("access_token")
        return {
            "Authorization": "Bearer " + access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def make_requests(self):
        return {
            "dateRanges": {
                "startDate": self.start,
                "endDate": self.end,
            },
            "viewId": self.ga_view_id,
            "dimensions": [
                {"name": f"ga:{dimension}"} for dimension in self.dimensions
            ],
            "metrics": [{"expression": f"ga:{metric}"} for metric in self.metrics],
            "pageSize": "50000",
        }

    def fetch(self):
        rows = []
        payload = {"reportRequests": [report.make_requests() for report in self.reports]}
        url = "https://analyticsreporting.googleapis.com/v4/reports:batchGet"
        while True:
            with self.sessions.post(
                url,
                headers=self.headers,
                json=payload,
            ) as r:
                res = r.json()
            report = res['reports'][0]
            column_header = report['columnHeader']
            data = report['data']
            _rows = data['rows']
            rows.extend(_rows)
            next_page_token = data.get('nextPageToken', None)
            if next_page_token:
                payload['reportRequests'][0]['pageToken'] = next_page_token
            else:
                break
        return column_header, rows


class DemographicsReport(UAReports):
    def __init__(self, ga_view_id, start, end):
        super().__init__(ga_view_id, start, end)

    def get_dimensions(self):
        return ["date", "userType", "country", "deviceCategory", "userAgeBracket"]

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


class AcquisitionsReport(UAReports):
    def __init__(self, ga_view_id, start, end):
        super().__init__(ga_view_id, start, end)

    def get_dimensions(self):
        return [
            "date",
            "clientId",
            "channelGrouping",
            "socialNetwork",
            "fullReferrer",
            "pagePath",
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


class UAJobs:
    def __init__(self, ga_view_id, start, end):
        self.ga_view_id = ga_view_id
        self.start = start
        self.end = end
        self.sessions = self.init_session()
        self.headers = self.create_header()
        self.reports = [
            UAReports.create(self.ga_view_id, i, self.start, self.end)
            for i in ["demographics", "acquisitions"]
        ]

    def init_session(self):
        return requests.Session()

    def create_header(self):
        params = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
            "grant_type": "refresh_token",
        }
        with self.sessions.post(
            "https://oauth2.googleapis.com/token", params=params
        ) as r:
            access_token = r.json().get("access_token")
        return {
            "Authorization": "Bearer " + access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def fetch(self):
        rows = []
        payload = {"reportRequests": [report.make_requests() for report in self.reports]}
        url = "https://analyticsreporting.googleapis.com/v4/reports:batchGet"
        while True:
            with self.sessions.post(
                url,
                headers=self.headers,
                json=payload,
            ) as r:
                res = r.json()
            report = res['reports'][0]
            column_headers = report['columnHeader']
            data = report['data']
            _rows = data['rows']
            rows.extend(_rows)
            next_page_token = data.get('nextPageToken', None)
            if next_page_token:
                payload['reportRequests'][0]['pageToken'] = next_page_token
            else:
                break
        return column_headers, rows

    def transform(self, column_headers, rows):
        dimension_headers = column_headers["dimensions"]
        metric_headers = column_headers['metricHeader']['metricHeaderEntries']
        


    # def transform(self):
    #     if results.get("reports")[0].get("data").get("rows") is None:
    #                 break
    #             report = results.get("reports")[0]
    #             column_headers = report.get("columnHeader")
    #             dimension_headers = column_headers.get("dimensions")
    #             dimension_headers = list(
    #                 map(lambda x: x.split(":")[1], dimension_headers)
    #             )
    #             metric_header = column_headers.get("metricHeader").get(
    #                 "metricHeaderEntries"
    #             )
    #             for metric in metric_header:
    #                 metric["name"] = metric["name"].split(":")[1]
    #             for row in report.get("data").get("rows"):
    #                 row_json = {}
    #                 dimensions = row.get("dimensions")
    #                 metrics = row.get("metrics")
    #                 for header, dimension in zip(dimension_headers, dimensions):
    #                     row_json[header] = dimension
    #                 for i in metrics:
    #                     for metricHeader, value in zip(metric_header, i.get("values")):
    #                         row_json[metricHeader.get("name")] = value
    #                 rows.append(row_json)

    #     for i in tqdm(rows):
    #         i["date"] = datetime.strptime(i["date"], "%Y%m%d").strftime("%Y-%m-%d")

    # async def fetch_reports(self):
    #     transaction_report = await self.fetch_report(self.reports.get("transactions"))
    #     sessions_report = await self.fetch_report(self.reports.get("sessions"))

    #     client_ids = [i["clientId"] for i in transaction_report]
    #     client_ids = list(set(client_ids))
    #     key_func = lambda x: x["date"]

    #     transaction_report_grouped = [
    #         {
    #             "key": key,
    #             "path": self.reports.get("transactions").get("path"),
    #             "value": "\n".join([json.dumps(i) for i in group]),
    #         }
    #         for key, group in itertools.groupby(
    #             sorted(transaction_report, key=key_func), key_func
    #         )
    #     ]
    #     sessions_report_grouped = [
    #         {
    #             "key": key,
    #             "path": self.reports.get("sessions").get("path"),
    #             "value": "\n".join([json.dumps(i) for i in group]),
    #         }
    #         for key, group in itertools.groupby(
    #             sorted(sessions_report, key=key_func), key_func
    #         )
    #     ]

    #     for rows_grouped in [transaction_report_grouped, sessions_report_grouped]:
    #         async with Storage(session=aiohttp.ClientSession()) as storage_client:
    #             tasks = [
    #                 asyncio.create_task(
    #                     self.upload(
    #                         storage_client, row["path"], row["key"], row["value"]
    #                     )
    #                 )
    #                 for row in rows_grouped
    #             ]
    #             _ = [
    #                 await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))
    #             ]

    #     return client_ids

    # async def upload(self, storage_client, path, filename, data):
    #     _ = await storage_client.upload(
    #         self.bucket, path + filename + ".json", data, timeout=60
    #     )

    # async def fetch_activity(self, client_id, sessions, storage_client):
    #     body = {
    #         "dateRange": {
    #             "startDate": self.earliest_date,
    #             "endDate": self.end_date,
    #         },
    #         "viewId": self.ga_view_id,
    #         "pageSize": 50000,
    #         "user": {"type": "CLIENT_ID", "userId": client_id},
    #     }
    #     next_page_token = str(0)
    #     empty_sessions = False
    #     activities = []
    #     while next_page_token != None:
    #         body["pageToken"] = next_page_token
    #         async with sessions.post(
    #             "https://analyticsreporting.googleapis.com/v4/userActivity:search",
    #             headers=self.headers,
    #             json=body,
    #         ) as r:
    #             results = await r.json()
    #         page_sessions = results.get("sessions")
    #         if page_sessions == None:
    #             empty_sessions = True
    #             break
    #         activities.append(page_sessions)
    #         next_page_token = results.get("nextPageToken")

    #     if empty_sessions == False:
    #         activities = [i for j in activities for i in j]
    #         activities = [dict(item, **{"client_id": client_id}) for item in activities]

    #         _ = await self.upload(
    #             storage_client,
    #             self.activities.get("path"),
    #             client_id,
    #             "\n".join([json.dumps(i) for i in activities]),
    #         )

    # async def fetch_activities(self, client_ids):
        # async with aiohttp.ClientSession(
        #     connector=aiohttp.TCPConnector(limit=9)
        # ) as session, Storage(session=aiohttp.ClientSession()) as storage_client:
        #     tasks = [
        #         asyncio.create_task(
        #             self.fetch_activity(client_id, session, storage_client)
        #         )
        #         for client_id in client_ids
        #     ]
        #     _ = [await f for f in tqdm(asyncio.as_completed(tasks), total=len(tasks))]

    def run(self):
        client_ids = self.fetch()
        # await self.fetch_activities(client_ids)
        # response = {
            # "start_date": self.start_date,
            # "end_date": self.end_date,
            # "num_processed": len(client_ids),
        # }
        # print(response)
        # return response
