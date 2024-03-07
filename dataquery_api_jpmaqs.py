"""
A Python wrapper for the JPMorgan DataQuery API.

This script is meant as a guide to using the JPMorgan DataQuery API.

This module does not contain any error handling, and will break if any errors are raised.

For JPMaQS specific functionality, see the :

- `Macrosynergy package documentation <https://macrosynergy.readthedocs.io/>`_.

- `macrosynergy.download documentation. <https://macrosynergy.readthedocs.io/stable/macrosynergy.download.html>`_.

- `Macrosynergy package on GitHub <https://github.com/macrosynergy/macrosynergy>`_.

"""

try:
    import concurrent.futures
    import logging
    import os
    import json
    from datetime import datetime as datetime
    from datetime import timedelta, timezone
    import time
    import functools
    from typing import Dict, Generator, Iterable, List, Optional, Union, overload

    import pandas as pd
    import requests
    import requests.compat
    from tqdm import tqdm
except ImportError as e:
    print(f"Import Error: {e}")
    print(
        "Please install the required packages in your Python "
        "environment using the following command:"
    )
    print("\n\t python -m pip install pandas requests tqdm\n")
    raise e


logger = logging.getLogger(__name__)


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
MAX_RETRY: int = 3  # Maximum number of retries for failed requests


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


# @overload
# def construct_jpmaqs_expressions(
#     ticker: List[str], metrics: List[str]
# ) -> List[str]: ...


# @overload
# def construct_jpmaqs_expressions(ticker: str, metrics: List[str]) -> List[str]: ...


def construct_jpmaqs_expressions(
    ticker: Union[str, List[str]],
    metrics: List[str] = ["value", "grading", "eop_lag", "mop_lag"],
) -> List[str]:
    """
    Construct a list of expressions for the JPMaQS group.

    :param <str|List[str]> ticker: ticker or list of tickers to construct expressions for.
    :param <List[str]> metrics: list of metrics to construct expressions for.
    :return <List[str]>: list of expressions.
    """
    if isinstance(ticker, str):
        return [f"DB(JPMAQS,{ticker},{metric})" for metric in metrics]
    assert isinstance(ticker, list), "ticker must be a string or a list of strings."
    return [f"DB(JPMAQS,{t},{metric})" for t in ticker for metric in metrics]


def time_series_to_df(dicts_list: List[Dict]) -> pd.DataFrame:
    """
    Convert a list of timeseries dictionaries/JSONs to a pandas DataFrame.

    :param dicts_list <list>: List of dictionaries containing time series
        data from the DataQuery API
    Returns
    :return <pd.DataFrame>: DataFrame containing the data
    """
    if isinstance(dicts_list, dict):
        dicts_list = [dicts_list]

    expressions = [d["attributes"][0]["expression"] for d in dicts_list]
    return_df = pd.concat(
        [
            pd.DataFrame(
                dicts_list.pop()["attributes"][0]["time-series"],
                columns=["real_date", "value"],
            ).assign(expression=expressions.pop())
            for _ in range(len(dicts_list))
        ],
        axis=0,
    ).reset_index(drop=True)[["real_date", "expression", "value"]]
    return_df["real_date"] = pd.to_datetime(return_df["real_date"])
    return return_df


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

    :param url <str>: URL to make request to
    :param params <dict>: Parameters to pass to request

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
            raise Exception(
                f"Request failed with status code {response.status_code}.\n"
                f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}\n"
                f"Response : {response.text}\n"
                f"URL: {form_full_url(url, params)}"
                f"Request headers: {headers}\n"
            )

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
        base_url: str = OAUTH_BASE_URL,
        dq_resource_id: Optional[str] = OAUTH_DQ_RESOURCE_ID,
    ):
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.proxy: str = proxy
        self.dq_resource_id: str = dq_resource_id
        self.current_token: Optional[Dict] = None
        self.base_url: str = base_url
        self.token_data: Dict = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "aud": self.dq_resource_id,
        }

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs): ...

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

    def heartbeat(self, raise_error: bool = False) -> bool:
        """
        Check if the DataQuery API is up.

        :param raise_error <bool>: Whether to raise an exception if the API is down.

        Returns
        :return <bool>: True if up, False otherwise
        """
        url: str = self.base_url + HEARTBEAT_ENDPOINT
        response: requests.Response = self._request(
            url=url, params={"data": "NO_REFERENCE_DATA"}
        )

        result = "info" in response
        if not result and raise_error:
            raise Exception(
                f"DataQuery API Heartbeat failed. \n Response : {response} \n"
                f"User ID: {self.get_access_token()['user_id']}\n"
                f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}"
            )

        return result

    def _fetch(self, url: str, params: dict, **kwargs) -> List[Dict]:
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
                self._fetch(
                    url=self.base_url + response["links"][1]["next"],
                    params={},
                    **kwargs,
                )
            )

        return downloaded_data

    def get_catalogue(
        self,
        group_id: str = JPMAQS_GROUP_ID,
        verbose: bool = True,
        show_progress: bool = True,
    ) -> List[str]:
        """
        Method to get the JPMaQS catalogue.
        Queries the DataQuery API's Groups/Search endpoint to get the list of
        tickers in the JPMaQS group. The group ID can be changed to fetch a
        different group's catalogue.

        :param <str> group_id: the group ID to fetch the catalogue for.

        :return <List[str]>: list of tickers in the JPMaQS group.

        :raises <ValueError>: if the response from the server is not valid.
        """
        if verbose:
            print(f"Downloading the {group_id} catalogue from DataQuery...")
        try:
            response_list: Dict = self._fetch(
                url=self.base_url + CATALOGUE_ENDPOINT,
                params={"group-id": group_id},
            )
            if show_progress:
                print()
        except Exception as e:
            raise e

        tickers: List[str] = [d["instrument-name"] for d in response_list]
        utkr_count: int = len(tickers)
        tkr_idx: List[int] = sorted([d["item"] for d in response_list])

        if not (
            (min(tkr_idx) == 1)
            and (max(tkr_idx) == utkr_count)
            and (len(set(tkr_idx)) == utkr_count)
        ):
            raise ValueError("The downloaded catalogue is corrupt.")

        return tickers

    def _get_result(
        self,
        url: str,
        params: dict,
        save_to_path: str,
        **kwargs,
    ) -> List[str]:
        """
        Save the downloaded timeseries data to a file or return it as a list of dictionaries.

        :param url <str>: URL to make request to.
        :param params <dict>: Parameters to pass to request.
        :param save_to_path <str>: Path to save the file to.
        """

        timeseries_list = self._fetch(url, params, **kwargs)
        if save_to_path is None:
            return timeseries_list

        if not os.path.exists(save_to_path):
            os.makedirs(save_to_path, exist_ok=True)

        results = []
        while len(timeseries_list) > 0:
            ts = timeseries_list.pop(0)
            if ts["attributes"][0]["time-series"] is None:
                continue
            expr = ts["attributes"][0]["expression"]
            pth = os.path.join(save_to_path, f"{expr}.csv")
            (
                pd.DataFrame(
                    ts["attributes"][0]["time-series"],
                    columns=["real_date", "value"],
                )
                .dropna()
                .to_csv(pth, index=False)
            )
            results.append(pth)

        return results

    def _get_timeseries(
        self,
        expressions: List[str],
        params: Dict,
        as_dataframe: bool = True,
        save_to_path: Optional[str] = None,
        max_retry: int = MAX_RETRY,
        show_progress: bool = True,
        **kwargs,
    ) -> List[Dict]:
        """
        Download data from the DataQuery API.


        :param expressions <list>: List of expressions to download
        :param params <dict>: Dictionary of parameters to pass to the request
        Returns
        :return <list>: List of dictionaries containing data
        """
        if max_retry < 0:
            raise Exception("Maximum number of retries reached.")

        expr_batches: List[List[str]] = [
            [expressions[i : min(i + EXPR_LIMIT, len(expressions))]]
            for i in range(0, len(expressions), EXPR_LIMIT)
        ]

        downloaded_data: List[Union[Dict, pd.DataFrame]] = []
        failed_batches: List[List[str]] = []
        if self.heartbeat(raise_error=True):
            print(f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}")
            print("Connected to DataQuery API!")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures: List[concurrent.futures.Future] = []
            for expr_batch in tqdm(
                expr_batches,
                desc="Requesting data",
                disable=not show_progress,
                total=len(expr_batches),
            ):
                current_params: Dict = params.copy()
                current_params["expressions"] = expr_batch
                curr_url: str = self.base_url + TIMESERIES_ENDPOINT
                futures.append(
                    executor.submit(
                        self._get_result,
                        url=curr_url,
                        params=current_params,
                        save_to_path=save_to_path,
                    )
                )
                time.sleep(API_DELAY_PARAM)
            for ix, future in tqdm(
                enumerate(futures),
                desc="Downloading data",
                disable=not show_progress,
                total=len(futures),
            ):
                try:
                    result = future.result()
                    if save_to_path is not None:
                        if not all(result):
                            raise Exception(
                                f"Failed to save data to path `{save_to_path}` for batch {ix}."
                            )
                    downloaded_data.extend(result)

                except Exception as e:
                    failed_batches.append(expr_batches[ix])
                    logger.error(f"Failed to download data for batch {ix} : {e}")

        if len(failed_batches) > 0:
            retry_exprs: List[str] = [
                expr for batch in failed_batches for expr in batch
            ]
            if max_retry > 0:
                print(
                    f"Retrying failed expressions: {retry_exprs};",
                    f"\nRetries left: {max_retry}",
                )
                return self._get_timeseries(
                    expressions=retry_exprs,
                    params=params,
                    as_dataframe=as_dataframe,
                    save_to_path=save_to_path,
                    max_retry=max_retry - 1,
                    show_progress=show_progress,
                    **kwargs,
                )
            else:
                print(
                    f"Failed to download data for expressions: {retry_exprs}",
                    "\nMaximum number of retries reached, skipping failed expressions.",
                )
                return []

        return downloaded_data

    def download(
        self,
        expressions: List[str],
        start_date: str,
        end_date: str,
        as_dataframe: bool = True,
        path: Optional[str] = None,
        show_progress: bool = False,
        calender: str = "CAL_WEEKDAYS",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
    ) -> Union[List[Dict], List[str], pd.DataFrame]:
        """
        Download data from the DataQuery API.

        :param expressions <List[str]>: List of expressions to download.
        :param start_date <str>: Start date of data to download (format: "YYYY-MM-DD").
            Defaults to "1990-01-01".
        :param end_date <Optional[str]>: End date of data to download
            (format: "YYYY-MM-DD"). Defaults to ``None``.
        :param as_dataframe <bool>: Whether to return the data as a Pandas DataFrame,
            or as a list of dictionaries. Defaults to True, returning a DataFrame.
        :param show_progress <bool>: Whether to show a progress bar for the download.
            Defaults to False.
        :param calender <str>: Calendar setting from DataQuery's specifications.
        :param frequency <str>: Frequency setting from DataQuery's specifications.
        :param conversion <str>: Conversion setting from DataQuery's specifications.
        :param nan_treatment <str>: NaN treatment setting from DataQuery's specifications.

        :return <Union[List[Dict], List[str], pd.DataFrame]>:
            - List of dictionaries (if as_dataframe=False).
            - List of dictionaries containing file paths in the form
                ``[{"expression": "some_expr", "file": "path/to/some_expr.csv"}, ...]``.
                (if save_to_path is provided).
            - Pandas DataFrame with columns ["real_date", "expression", "value"]
                (if as_dataframe=True, default).
        """
        if path is not None:
            path = os.path.expanduser(path)
            os.makedirs(os.path.normpath(path), exist_ok=True)

        if end_date is None:
            end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

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
        downloaded_data: Union[List[Dict], List[str]] = self._get_timeseries(
            expressions=expressions,
            params=params_dict,
            as_dataframe=as_dataframe,
            save_to_path=path,
            show_progress=show_progress,
        )
        print(
            f"Download done."
            f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}"
        )
        if path:
            assert all(isinstance(f, str) for f in downloaded_data)
            print(f"Data saved to {path}.")
            print(f"Downloaded {len(downloaded_data)} / {len(expressions)} files.")
            result = [
                {
                    "expression": str(os.path.basename(f)).split(".")[0],
                    "file": str(os.path.abspath(os.path.normpath(f))).replace(
                        "\\", "/"
                    ),
                }
                for f in downloaded_data
            ]
            logger.info(f"Data saved to {path}.")
            logger.info(f"Saved files: {result}")
            return result

        mismm = "Expression not found; No message available."
        missing_exprs = [
            (
                expr["attributes"][0]["expression"],
                expr["attributes"][0].get("message", mismm),
            )
            for expr in downloaded_data
            if expr["attributes"][0]["time-series"] is None
        ]

        if len(missing_exprs) > 0:
            logger.warning(f"Missing expressions: {missing_exprs}")
            print(
                f"Missing expressions: {missing_exprs}\n"
                f"Downloaded {len(downloaded_data) - len(missing_exprs)}"
                f" / {len(expressions)} expressions."
            )
            downloaded_data = [
                expr
                for expr in downloaded_data
                if expr["attributes"][0]["time-series"] is not None
            ]

        if as_dataframe:
            return time_series_to_df(downloaded_data)

        return downloaded_data


def download_all_jpmaqs_to_disk(
    client_id: str,
    client_secret: str,
    proxy: Optional[Dict] = None,
    path="./data",
    show_progress: bool = False,
    start_date: str = "1990-01-01",
    end_date: Optional[str] = None,
):
    """
    Download all JPMaQS data to disk.

    :param client_id <str>: Client ID for the DataQuery API.
    :param client_secret <str>: Client secret for the DataQuery API.
    :param path <str>: Path to save the data to.
    :param start_date <str>: Start date of data to download.
    :param end_date <str>: End date of data to download.
    """
    if not isinstance(path, str):
        raise ValueError("`path` must be a string.")

    path = os.path.join(path, "JPMaQSDATA").replace("\\", "/")
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    data: List[Dict[str, str]] = []  # [{expression:file}, {expression:file}, ...]
    with DQInterface(
        client_id=client_id,
        client_secret=client_secret,
        proxy=proxy,
    ) as dq:
        assert dq.heartbeat(), "DataQuery API Heartbeat failed."
        tickers = dq.get_catalogue()
        expressions = construct_jpmaqs_expressions(tickers)
        data: pd.DataFrame = dq.download(
            expressions=expressions,
            start_date=start_date,
            end_date=end_date,
            path=path,
            show_progress=show_progress,
        )

    for dx in data:
        if not os.path.exists(dx["file"]):
            raise FileNotFoundError(
                f"File {dx['file']} for expression {dx['expression']} not found."
            )


# CLI and example usage


def example_usage(
    client_id: str, client_secret: str, proxy: Optional[Dict] = None, test_path=None
):
    """
    Example usage of the DQInterface class.
    Click "[source]" to see the code.
    """
    expressions = [
        "DB(CFX,USD,)",
        "DB(CFX,AUD,)",
        "DB(CFX,GBP,)",
        "DB(JPMAQS,USD_EQXR_VT10,value)",
        "DB(JPMAQS,EUR_EQXR_VT10,value)",
        "DB(JPMAQS,AUD_EXALLOPENNESS_NSA_1YMA,value)",
        "DB(JPMAQS,AUD_EXALLOPENNESS_NSA_1YMA,grading)",
        "DB(JPMAQS,GBP_EXALLOPENNESS_NSA_1YMA,eop_lag)",
        "DB(JPMAQS,GBP_EXALLOPENNESS_NSA_1YMA,mop_lag)",
    ]

    with DQInterface(
        client_id=client_id,
        client_secret=client_secret,
        proxy=proxy,
    ) as dq:
        assert dq.heartbeat(), "DataQuery API Heartbeat failed."
        data: pd.DataFrame = dq.download(
            expressions=expressions,
            start_date="2023-02-20",
            end_date="2023-03-01",
            path=test_path,
        )

        if not test_path:
            assert isinstance(data, pd.DataFrame)
            print(data.head(20))


def heartbeat_test(client_id: str, client_secret: str, proxy: Optional[Dict] = None):
    """
    Test the DataQuery API heartbeat.

    :param client_id <str>: Client ID for the DataQuery API.
    :param client_secret <str>: Client secret for the DataQuery API.
    :param proxy <dict>: Dictionary containing the proxy settings.

    :return <bool>: True if the heartbeat is successful, False otherwise.
    """

    with DQInterface(
        client_id=client_id, client_secret=client_secret, proxy=proxy
    ) as dq:
        start = time.time()
        hb = dq.heartbeat()
        end = time.time()
        if not hb:
            print(
                f"Connection to DataQuery API failed."
                "Retrying and logging printing error to stdout."
            )
            start = time.time()
            dq.heartbeat(raise_error=True)
            end = time.time()

        if hb:
            print(f"Connection to DataQuery API")
            print(f"Authentication + Heartbeat took {end - start:.2f} seconds.")


def get_credentials(file: str) -> Dict:
    """
    Get the credentials from a JSON file.

    :param file <str>: Path to the credentials JSON file.

    :return <dict>: Dictionary containing the credentials.
    """
    emsg = "`{cred}` not found in the credentials file ('" + file + "')."
    cks = ["client_id", "client_secret"]
    with open(file, "r") as f:
        res: dict = json.load(f)
        for ck in cks:
            if ck not in res.keys():
                raise ValueError(emsg.format(cred=ck))
        if not isinstance(res.get("proxy", {}), dict):
            raise ValueError("`proxy` must be a dictionary.")

        res = {a: res[a] for a in (cks + ["proxy"]) if res.get(a, None) is not None}

        return res


def cli():
    """
    CLI Arguments to download JPMaQS data.

    Usage:

    .. code-block:: bash

        python dataquery_api.py --credentials <path_to_credentials> --path <path_to_save_data> --progress <bool>

    Your credentials file should look like this (proxy is optional):

    .. code-block:: python

        {
            "client_id": "your_client_id",
            "client_secret": "your_client_secret"
            "proxy": {
                "https": "https://your_proxy:port",
                }
        }

    :param credentials <str>: Path to the credentials JSON (``--credentials``).
    :param path <str>: Path to save the data to (``--path``). If not provided, only a few
        timeseries will be downloaded as a DataFrame and printed. This is only for testing.
    :param progress <bool>: Whether to show a progress bar for the download (``--progress``).
    """
    import argparse

    parser = argparse.ArgumentParser(description="Download JPMaQS data.")

    parser.add_argument(
        "--credentials",
        type=str,
        help="Path to the credentials JSON.",
        # required=True,
        default="credentials.json",
    )
    parser.add_argument(
        "--path",
        type=str,
        help="Path to save the data to.",
        required=False,
        default="~/tickers/",
    )

    parser.add_argument(
        "--test-path",
        type=str,
        help="Path to save the data to, for testing functionality.",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--heartbeat",
        action="store_true",
        help="Test the DataQuery API heartbeat and exit.",
        required=False,
        default=False,
    )

    parser.add_argument(
        "--progress",
        action="store_true",
        help="Whether to show a progress bar for the download.",
        required=False,
        default=True,
    )

    args = parser.parse_args()
    creds = get_credentials(args.credentials)

    heartbeat_test(**creds)
    if args.heartbeat:
        return

    if args.path is None:
        example_usage(**creds, test_path=args.test_path)
    else:
        download_all_jpmaqs_to_disk(
            **creds, path=args.path, show_progress=args.progress
        )


if __name__ == "__main__":
    cli()
