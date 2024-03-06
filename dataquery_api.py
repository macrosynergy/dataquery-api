"""A Python wrapper for the JPMorgan DataQuery API.
This script is meant as a guide to using the JPMorgan DataQuery API.
This module does not contain any error handling, and will break if any errors are raised.
For JPMaQS specific functionality, see the macrosynergy.download module.

Macrosynergy Package : https://github.com/macrosynergy/macrosynergy

The Macrosynergy package download module : 
https://github.com/macrosynergy/macrosynergy/tree/develop/macrosynergy/download

"""

try:
    from typing import List, Optional, Dict, Union
    import requests, requests.compat
    from datetime import datetime as datetime, timezone, timedelta
    from time import sleep
    import pandas as pd
except ImportError as e:
    print(f"Import Error: {e}")
    print(
        "Please install the required packages in your Python "
        "environment using the following command:"
    )
    print("\n\t python -m pip install pandas requests\n")

# Constants. WARNING : DO NOT MODIFY.
OAUTH_BASE_URL: str = (
    "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
)
TIMESERIES_ENDPOINT: str = "/expressions/time-series"
HEARTBEAT_ENDPOINT: str = "/services/heartbeat"
CATALOGUE_ENDPOINT: str = "/group/instruments"
OAUTH_TOKEN_URL: str = "https://authe.jpmchase.com/as/token.oauth2"
OAUTH_DQ_RESOURCE_ID: str = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
API_DELAY_PARAM: float = 0.2  # 200ms delay between requests.
TOKEN_EXPIRY_BUFFER: float = 0.9  # 90% of token expiry time.
EXPR_LIMIT: int = 20  # Maximum number of expressions per request (not per "download").
JPMAQS_GROUP_ID: str = "JPMAQS"


def form_full_url(url: str, params: Dict = {}) -> str:
    """
    Forms a full URL from a base URL and a dictionary of parameters.
    Useful for logging and debugging.

    :param <str> url: base URL.
    :param <dict> params: dictionary of parameters.

    :return <str>: full URL
    """
    return requests.compat.quote(
        (f"{url}?{requests.compat.urlencode(params)}" if params else url),
        safe="%/:=&?~#+!$,;'@()*[]",
    )


def request_wrapper(
    url: str,
    headers: Optional[Dict] = None,
    params: Optional[Dict] = None,
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
        response: requests.Response = requests.request(
            method=method, url=url, params=params, headers=headers, **kwargs
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
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.proxy: str = proxy
        self.dq_resource_id: str = dq_resource_id
        self.current_token: Optional[Dict] = None
        self.token_data: Dict = {
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

        def _is_active(token: Optional[dict] = None) -> bool:
            if token is None:
                return False
            expires: datetime = token["created_at"] + timedelta(
                seconds=token["expires_in"] * TOKEN_EXPIRY_BUFFER
            )
            return datetime.now() < expires

        # if the token is active (and valid), return it; else, make a request for a new token
        if _is_active(self.current_token):
            return self.current_token["access_token"]
        else:
            r_json = request_wrapper(
                url=OAUTH_TOKEN_URL,
                data=self.token_data,
                method="post",
                proxies=self.proxy,
            ).json()
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
        ).json()

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
        return "info" in response

    def _fetch_data(self, url: str, params: dict, **kwargs) -> requests.Response:
        downloaded_data: List[Dict] = []
        response: Dict = self._request(url=url, params=params, **kwargs)

        if (response is None) or ("instruments" not in response.keys()):
            if response is not None:
                if (
                    ("info" in response)
                    and ("code" in response["info"])
                    and (int(response["info"]["code"]) == 204)
                ):
                    raise Exception(
                        f"Content was not found for the request: {response}\n"
                        f"User ID: {self.get_access_token()['user_id']}\n"
                        f"URL: {form_full_url(url, params)}\n"
                        f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}"
                    )

            raise Exception(
                f"Invalid response from DataQuery: {response}\n"
                f"User ID: {self.get_access_token()['user_id']}\n"
                f"URL: {form_full_url(url, params)}"
                f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}"
            )

        downloaded_data.extend(response["instruments"])

        if "links" in response.keys() and response["links"][1]["next"] is not None:
            downloaded_data.extend(
                self._fetch_data(
                    url=OAUTH_BASE_URL + response["links"][1]["next"],
                    params={},
                )
            )

        return downloaded_data

    def download(
        self,
        expressions: List[str],
        start_date: str,
        end_date: str,
        as_dataframe: bool = True,
        calender: str = "CAL_WEEKDAYS",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
    ) -> Union[List[Dict], pd.DataFrame]:
        """
        Download data from the DataQuery API.
        Parameters
        :param expressions <list>: List of expressions to download
        :param start_date <str>: Start date of data to download
        :param end_date <str>: End date of data to download
        :param as_dataframe <bool>: Whether to return the data as a Pandas DataFrame,
            or as a list of dictionaries. Defaults to True, returning a DataFrame.
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

        params_dict: Dict = {
            "format": "JSON",
            "start-date": start_date,
            "end-date": end_date,
            "calendar": calender,
            "frequency": frequency,
            "conversion": conversion,
            "nan_treatment": nan_treatment,
            "data": "NO_REFERENCE_DATA",
        }

        expr_batches: List[List[str]] = [
            [expressions[i : min(i + EXPR_LIMIT, len(expressions))]]
            for i in range(0, len(expressions), EXPR_LIMIT)
        ]
        invalid_response_msg: str = "Invalid response from DataQuery API."
        heartbeat_failed_msg: str = "DataQuery API Heartbeat failed."

        downloaded_data: List[Dict] = []
        assert self.heartbeat(), heartbeat_failed_msg
        print("Heartbeat Successful.")

        for expr_batch in expr_batches:
            current_params: Dict = params_dict.copy()
            current_params["expressions"] = expr_batch
            curr_url: str = OAUTH_BASE_URL + TIMESERIES_ENDPOINT
            downloaded_data.extend(
                self._fetch_data(url=curr_url, params=current_params)
            )

        if as_dataframe:
            return time_series_to_df(downloaded_data)
        else:
            return downloaded_data


def time_series_to_df(dicts_list: List[Dict]) -> pd.DataFrame:
    """
    Convert the downloaded data to a pandas DataFrame.
    Parameters
    :param dicts_list <list>: List of dictionaries containing time series
        data from the DataQuery API
    Returns
    :return <pd.DataFrame>: DataFrame containing the data
    """
    dfs: List = []
    for d in dicts_list:
        df = pd.DataFrame(
            d["attributes"][0]["time-series"], columns=["real_date", "value"]
        )
        df["expression"] = d["attributes"][0]["expression"]
        dfs += [df]

    return_df = pd.concat(dfs, axis=0).reset_index(drop=True)[
        ["real_date", "expression", "value"]
    ]
    return_df["real_date"] = pd.to_datetime(return_df["real_date"])
    return return_df


if __name__ == "__main__":
    import os

    client_id: str = os.getenv("DQ_CLIENT_ID")
    client_secret: str = os.getenv("DQ_CLIENT_SECRET")

    dq: DQInterface = DQInterface(client_id, client_secret)
    assert dq.heartbeat(), "DataQuery API Heartbeat failed."

    expressions = [
        "DB(JPMAQS,USD_EQXR_VT10,value)",
        "DB(JPMAQS,AUD_EXALLOPENNESS_NSA_1YMA,value)",
        "DB(CFX,USD,)",
        "DB(CFX,AUD,)",
        "DB(CFX,GBP,)",
    ]
    start_date: str = "2020-01-25"
    end_date: str = "2023-02-05"

    data: pd.DataFrame = dq.download(
        expressions=expressions,
        start_date=start_date,
        end_date=end_date,
    )
    if isinstance(data, pd.DataFrame):
        print(data.head())
    else:
        print(data[: min(5, len(data))])
