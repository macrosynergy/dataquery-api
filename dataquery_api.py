"""A Python wrapper for the JPMorgan DataQuery API.
This script is meant as a guide to using the JPMorgan DataQuery API.
This module does not contain any error handling, and will break if any errors are raised.
For JPMaQS specific functionality, see the macrosynergy.download module.

Macrosynergy Package : https://github.com/macrosynergy/macrosynergy

The Macrosynergy package download module : 
https://github.com/macrosynergy/macrosynergy/tree/develop/macrosynergy/download

"""

import requests
from typing import List, Optional, Dict
from datetime import datetime
from tqdm import tqdm

# Constants. WARNING : DO NOT MODIFY.
OAUTH_BASE_URL: str = (
    "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
)
TIMESERIES_ENDPOINT: str = "/expressions/time-series"
HEARTBEAT_ENDPOINT: str = "/services/heartbeat"
OAUTH_TOKEN_URL: str = "https://authe.jpmchase.com/as/token.oauth2"
OAUTH_DQ_RESOURCE_ID: str = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
API_DELAY_PARAM: float = 0.3  # 300ms delay between requests.
EXPR_LIMIT: int = 20  # Maximum number of expressions per request (not per "download").


def request_wrapper(
    url: str,
    headers: Optional[Dict] = None,
    params: Optional[dict] = None,
    method: str = "get",
    **kwargs,
) -> requests.Response:
    """
    Wrapper function for requests.request() used to make a request
    to the JPMorgan DataQuery API.
    Parameters
    :param url <str>: URL to make request to
    :param params <dict>: Parameters to pass to request
    Returns
    :return <requests.Response>: Response object
    """
    # this function wraps the requests.request() method in a try/except block
    try:
        response = requests.request(
            method=method, 
            url=url, 
            params=params, 
            headers=headers, 
            **kwargs
        )
        # Check response
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Request failed with status code {response.status_code}.")
    except Exception as e:
        if isinstance(e, requests.exceptions.ProxyError):
            raise Exception("Proxy error. Check your proxy settings. Exception : ", e)
        elif isinstance(e, requests.exceptions.ConnectionError):
            raise Exception(
                "Connection error. Check your internet connection. Exception : ", e
            )
        else:
            raise e


class DQInterface:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        proxy: Optional[Dict] = None,
        dq_resource_id: Optional[str] = OAUTH_DQ_RESOURCE_ID,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.proxy = proxy
        self.dq_resource_id = dq_resource_id
        self.current_token: Optional[dict] = None
        self.token_data : dict = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "aud": self.dq_resource_id,
        }

    def get_access_token(self) -> str:
        """
        Helper function to verify if the current token is active and valid,
        and request a new one if it is not.
        Returns
        :return <str>: Access token
        """

        def is_active(token: Optional[dict] = None) -> bool:
            """
            Helper function to check if a token is active.
            Parameters
            :param token <dict>: Token to check. Can be None, which will return False.
            Returns
            :return <bool>: True if token is active, False otherwise
            """
            # return (token is None) or (datetime.now() - \
            # token["created_at"]).total_seconds() / 60 >= (token["expires_in"] - 1)
            if token is None:
                return False
            else:
                created: datetime = token["created_at"]
                expires: int = token["expires_in"]
                return ((datetime.now() - created).total_seconds() / 60) >= (expires - 1)

        # if the token is active (and valid), return it; else, make a request for a new token
        if is_active(self.current_token):
            return self.current_token["access_token"]
        else:
            response = request_wrapper(
                url=OAUTH_TOKEN_URL,
                data=self.token_data,
                method="post",
                proxies=self.proxy,
            )
            r_json = response.json()
            self.current_token = {
                "access_token": r_json["access_token"],
                "created_at": datetime.now(),
                "expires_in": r_json["expires_in"],
            }
            return self.current_token["access_token"]

    def _request(self, url: str, params: dict, **kwargs) -> requests.Response:
        """
        Helper function to make a request to the DataQuery API.
        Parameters
        :param url <str>: URL to make request to
        :param params <dict>: Parameters to pass to request
        Returns
        :return <requests.Response>: Response object
        """
        # Make request using wrapper function
        # this funciton wraps the request wrapper to add the access token
        # and add the proxy to all requests from this class
        return request_wrapper(
            url=url,
            params=params,
            headers={"Authorization": f"Bearer {self.get_access_token()}"},
            method="get",
            proxies=self.proxy,
            **kwargs,
        )

    def heartbeat(self) -> bool:
        """
        Check if the DataQuery API is up.
        Returns
        :return <bool>: True if up, False otherwise
        """
        response: requests.Response = self._request(
            url=OAUTH_BASE_URL + HEARTBEAT_ENDPOINT,
            params={"data": "NO_REFERENCE_DATA"},
        )
        # no need for response.ok because
        # response.status_code==200 is checked in the wrapper
        return "info" in response.json()

    def download(
        self,
        expressions: List[str],
        start_date: str,
        end_date: str,
        calender: str = "CAL_ALLDAYS",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        run_sequential: bool = False,
    ) -> List[Dict]:
        """
        Download data from the DataQuery API.
        Parameters
        :param expressions <list>: List of expressions to download
        :param start_date <str>: Start date of data to download
        :param end_date <str>: End date of data to download
        :param calender <str>: Calendar setting from DataQuery's specifications
        :param frequency <str>: Frequency setting from DataQuery's specifications
        :param conversion <str>: Conversion setting from DataQuery's specifications
        :param nan_treatment <str>: NaN treatment setting from DataQuery's specifications
        :param run_sequential <bool>: Whether to run the download
            sequentially or as multithreaded requests.
            Defaults to False (multithreaded recommended).
        Returns
        :return <list>: List of dictionaries containing data
        """

        params = {
            "format": "JSON",
            "start_date": start_date,
            "end_date": end_date,
            "calendar": calender,
            "frequency": frequency,
            "conversion": conversion,
            "nan_treatment": nan_treatment,
            "data": "NO_REFERENCE_DATA",
        }

        expr_batches: List[List[str]] = [
            expressions[i : min(i + EXPR_LIMIT, len(expressions))]
            for i in range(0, len(expressions), EXPR_LIMIT)
        ]

        downloaded_data: List[Dict] = []

        assert self.heartbeat(), "DataQuery API Heartbeat failed."

        for i, expr_batch in enumerate(expr_batches):
            params["expressions"] = expr_batch
            while "instruments" not in curr_download:
                response = self._request(
                    url=OAUTH_BASE_URL + TIMESERIES_ENDPOINT,
                    params=params,
                )
            curr_download = response.json()
            
            if "links" not in curr_download:
                raise Exception("Invalid response from DataQuery API.")
            else:
                    response = self._request(
                        url=curr_download["links"]["next"],
                        params=params,
                    )
                    curr_download = response.json()

        return downloaded_data


if __name__ == "__main__":
    import os

    client_id = os.environ["JPMAQS_API_CLIENT_ID"]
    client_secret = os.environ["JPMAQS_API_CLIENT_SECRET"]
    dq = DQInterface(client_id, client_secret)

    assert dq.heartbeat(), "DataQuery API Heartbeat failed."

    expressions = [
        "DB(JPMAQS,ALM_COCRY_NSA,value)",
        "DB(JPMAQS,USD_EQXR_VT10,value)",
        # "DB(JPMAQS,Metallica,value)",
        # "DB(JPMAQS,USD_EQXR_VT10,grading)",
        # "DB(JPMAQS,USD_EQXR_VT10,eop_lag)",
        # "DB(JPMAQS,USD_EQXR_VT10,mop_lag)",
        # "DB(JPMAQS,USD_INFTARGET_NSA,value)",
        # "DB(JPMAQS,USD_INFTARGET_NSA,grading)",
        # "DB(JPMAQS,USD_INFTARGET_NSA,eop_lag)",
        # "DB(JPMAQS,USD_INFTARGET_NSA,mop_lag)",
        # "DB(JPMAQS,ZAR_FXXR_NSA,value)",
        # "DB(JPMAQS,ZAR_FXXR_NSA,grading)",
        # "DB(JPMAQS,ZAR_FXXR_NSA,eop_lag)",
        # "DB(JPMAQS,ZAR_FXXR_NSA,mop_lag)",
    ]

    start_date = "2020-01-10"
    end_date = "2023-02-01"
    data = dq.download(expressions, start_date, end_date)
    print(data)