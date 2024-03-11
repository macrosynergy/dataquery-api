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
    import glob
    import json
    import logging
    import os
    import shutil
    import time
    from datetime import datetime, timedelta, timezone
    from typing import Dict, List, Optional, Union, overload

    import pandas as pd
    import requests
    import requests.compat
except ImportError as e:
    print(f"Import Error: {e}")
    print(
        "Please install the required packages in your Python "
        "environment using the following command:"
    )
    print("\n\t python -m pip install pandas requests tqdm\n")
    raise e


def mtqdm(*args, **kwargs):
    return args[0]


try:
    from tqdm import tqdm
except Exception as e:
    tqdm = mtqdm


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
MAX_CONSECUTIVE_FAILURES: int = 5


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


def UTCNOW() -> str:
    """
    Get the current time in UTC as a string.

    :return <datetime>: Current time in UTC, YYYY-MM-DD HH:MM:SS.msms
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]


@overload
def construct_jpmaqs_expressions(
    ticker: List[str], metrics: List[str]
) -> List[str]: ...


@overload
def construct_jpmaqs_expressions(ticker: str, metrics: List[str]) -> List[str]: ...


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


def save_ts_to_jpmaqs_csv(
    timeseries_list: List[Dict],
    path: str,
    drop_na: bool = True,
) -> List[Dict[str, str]]:
    """
    Saves the timeseries data to a CSV file in the JPMaQS format, with all data for a single ticker
    saved to a single file. Only accepts timeseries data for a single ticker.
    Returns a list of saved expressions.

    :param timeseries_list <list>: List of dictionaries containing timeseries data from the DataQuery API.
    :param path <str>: Path to save the data to.
    :param drop_na <bool>: Whether to drop rows with NaN values.
    :return <list>: List of dictionaries containing saved expressions and file paths.
    """

    getexprts = lambda d: d["attributes"][0]["expression"]
    getts = lambda d: d["attributes"][0]["time-series"]
    getmsg = lambda d: d["attributes"][0].get("message", None)
    splitexpr = lambda s: str(s).replace("DB(JPMAQS,", "").replace(")", "").split(",")
    getticker = lambda s: splitexpr(s)[0]
    getmetric = lambda s: splitexpr(s)[1]
    getcid = lambda s: getticker(s).split("_")[0]
    getxcat = lambda s: getticker(s).split("_", 1)[1]
    tickerpath = lambda t: os.path.join(path, getxcat(t), f"{t}.csv")

    def _create_ticker_csv(dict_list: List[Dict]) -> List[str]:
        assert (len(set(map(getticker, map(getexprts, dict_list)))) == 1) and all(
            getts(d) is not None for d in dict_list
        ), "All expressions must be for the same ticker."

        ticker = getticker(getexprts(dict_list[0]))
        _path = tickerpath(ticker)
        series: List[pd.DataFrame] = [
            pd.DataFrame(
                getts(d), columns=["real_date", getmetric(getexprts(d))]
            ).set_index("real_date")
            for d in dict_list
        ]
        if os.path.exists(_path):
            # if a ticker file exists, load all columns from it ad update them with the new data
            # delete the old file
            _new_mtrs = [s.columns[0] for s in series]
            lcsv: pd.DataFrame = pd.read_csv(_path, index_col="real_date")
            lcsv = lcsv.drop(columns=list(set(lcsv.columns) & set(_new_mtrs)))
            os.remove(_path)
            if len(lcsv.columns) > 0:
                series.append(lcsv)

        mtrs = [s.columns[0] for s in series]
        series = sorted(series, key=lambda x: x.columns[0])
        for s in series:
            s.index = pd.to_datetime(s.index).strftime("%Y-%m-%d")
        # dropna in rows where all values are NaN
        if drop_na:
            series = [s.dropna(how="all") for s in series]
            logger.info(f"Dropped NaN values for {ticker}.")

        pd.concat(series, axis=1).reset_index().to_csv(_path, index=False)

        return construct_jpmaqs_expressions(ticker, mtrs)

    # drop expressions with no timeseries data
    timeseries_list = list(filter(lambda d: getts(d) is not None, timeseries_list))
    tickers_in_ts = list(set([getticker(getexprts(_ts)) for _ts in timeseries_list]))
    all_found_expressions = list(map(getexprts, timeseries_list))
    saved_expressions = []
    for ticker in tickers_in_ts:
        os.makedirs(os.path.join(path, getxcat(ticker)), exist_ok=True)
        # _tslist = tsforticker(ticker)
        _tslist = list(
            filter(lambda d: getticker(getexprts(d)) == ticker, timeseries_list)
        )
        if len(_tslist) == 0:
            continue
        try:
            res = _create_ticker_csv(_tslist)
            saved_expressions += res
        except Exception as e:
            print(f"\nError creating csv for ticker {ticker} : {e}")
            raise e

    if len(set(all_found_expressions) - set(saved_expressions)) > 0:
        # existing exprs may not necessarily be in the list of all found exprs
        raise Exception(
            "Batch failed to save all expressions. "
            f"Saved expressions: {saved_expressions}, "
            f"Found expressions: {all_found_expressions}"
        )

    r = [
        {
            "expression": expr,
            "file": os.path.join(
                path, getxcat(getticker(expr)), f"{getticker(expr)}.csv"
            ),
        }
        for expr in all_found_expressions
    ]

    return r


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
                f"Timestamp (UTC): {UTCNOW()}\n"
                f"Response : {response.text}\n"
                f"URL: {form_full_url(url, params)}"
                f"Request headers: {headers}\n"
            )

    except Exception as e:
        if isinstance(e, requests.exceptions.ProxyError):
            raise Exception("Proxy error. Check your proxy settings. \nException : ", e)
        elif isinstance(e, requests.exceptions.ConnectionError):
            raise Exception(
                "Connection error. Check your internet connection. \nException : ", e
            )
        else:
            raise e


class DQInterface:
    """
    Class to interface with the JPMorgan DataQuery API (OAuth only).

    :param client_id <str>: Client ID for the DataQuery API.
    :param client_secret <str>: Client secret for the DataQuery API.
    :param proxy <Optional[Dict]>: Proxy settings for the request.
    :param batch_size <int>: Number of expressions to download per request. Defaults to 20,
        which is the maximum number allowed by the DataQuery API specifications.
    :param base_url <str>: Base URL for the DataQuery API.
    :param dq_resource_id <Optional[str]>: Resource ID for the DataQuery API.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        proxy: Optional[Dict] = None,
        batch_size: int = EXPR_LIMIT,
        base_url: str = OAUTH_BASE_URL,
        dq_resource_id: Optional[str] = OAUTH_DQ_RESOURCE_ID,
    ):
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.proxy: str = proxy
        self.dq_resource_id: str = dq_resource_id
        self.current_token: Optional[Dict] = None
        self.base_url: str = base_url
        self.batch_size: int = batch_size
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
        time.sleep(API_DELAY_PARAM)
        response: requests.Response = self._request(
            url=url, params={"data": "NO_REFERENCE_DATA"}
        )

        result = "info" in response
        if not result and raise_error:
            raise Exception(
                f"DataQuery API Heartbeat failed. \n Response : {response} \n"
                f"User ID: {self.get_access_token()['user_id']}\n"
                f"Timestamp (UTC): {UTCNOW()}"
            )

        return result

    def _fetch(self, url: str, params: dict, **kwargs) -> List[Dict]:
        """
        Fetch data from the DataQuery API, and catch any exceptions or errors with the response.

        :param url <str>: URL to make request to.
        :param params <dict>: Parameters to pass to request.
        :param kwargs: Additional keyword arguments to pass to the request.
        :return <List[Dict]>: List of dictionaries containing data.
        """

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
                        f"Timestamp (UTC): {UTCNOW()}"
                    )

            raise Exception(
                f"Invalid response from DataQuery: {response}\n"
                f"User ID: {self.get_access_token()['user_id']}\n"
                f"URL: {form_full_url(url, params)}"
                f"Timestamp (UTC): {UTCNOW()}"
            )

        downloaded_data.extend(response["instruments"])

        if "links" in response.keys() and response["links"][1]["next"] is not None:
            time.sleep(API_DELAY_PARAM)
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

        if verbose:
            print(f"Downloaded {utkr_count} tickers from the {group_id} catalogue.")

        return tickers

    def _get_result(
        self,
        url: str,
        params: dict,
        path: str,
        jpmaqs_formatting: bool = False,
        **kwargs,
    ) -> List[str]:
        """
        Save the downloaded timeseries data to a file or return it as a list of dictionaries.

        :param url <str>: URL to make request to.
        :param params <dict>: Parameters to pass to request.
        :param save_to_path <str>: Path to save the file to.
        """
        drop_na: bool = kwargs.pop("drop_na", False)

        timeseries_list = self._fetch(url, params, **kwargs)
        if path is None:
            return timeseries_list

        if jpmaqs_formatting:
            res = save_ts_to_jpmaqs_csv(
                timeseries_list=timeseries_list,
                path=path,
                drop_na=drop_na,
                **kwargs,
            )
            if len(res) != len(timeseries_list):
                if kwargs.get("ignore_errors", False):
                    raise Exception("Failed to save all expressions.")
            return res

        results = []
        while len(timeseries_list) > 0:
            ts = timeseries_list.pop(0)
            if ts["attributes"][0]["time-series"] is None:
                continue
            expr = ts["attributes"][0]["expression"]
            pth = os.path.join(path, f"{expr}.csv")
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
        path: Optional[str] = None,
        max_retry: int = MAX_RETRY,
        show_progress: bool = True,
        **kwargs,
    ) -> List[Dict]:
        """
        Download data from the DataQuery API.


        :param expressions <list>: List of expressions to download
        :param params <dict>: Dictionary of parameters to pass to the request
        :return <list>: List of dictionaries containing data
        """
        if max_retry < 0:
            raise Exception("Maximum number of retries reached.")

        expressions = sorted(expressions)

        expr_batches: List[List[str]] = [
            expressions[i : min(i + self.batch_size, len(expressions))]
            for i in range(0, len(expressions), self.batch_size)
        ]

        downloaded_data: List[Union[Dict, pd.DataFrame]] = []
        failed_batches: List[List[str]] = []
        continuous_failures = 0

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
                        path=path,
                        **kwargs,
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
                    if path is not None:
                        if not all(result):
                            raise Exception(
                                f"Failed to save data to path `{path}` for batch {ix}."
                            )
                    downloaded_data.extend(result)
                    continuous_failures = 0

                except Exception as e:
                    if isinstance(e, KeyboardInterrupt):
                        print("\n\t------ Keyboard Interrupt ------\n\t")
                        print("Aborting download.")
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise e

                    continuous_failures += 1
                    if continuous_failures > MAX_CONSECUTIVE_FAILURES:
                        raise Exception(
                            "Too many consecutive failures. Aborting download."
                        )

                    failed_batches.append(expr_batches[ix])
                    logger.error(
                        f"Failed to download data for batch with expressions: {expr_batches[ix]}"
                    )

        if len(failed_batches) > 0:
            retry_exprs: List[str] = [
                expr for batch in failed_batches for expr in batch
            ]
            if max_retry > 0:
                print(
                    f"Retrying failed expressions: {retry_exprs};",
                    f"\nRetries left: {max_retry}",
                )
                retried_output = self._get_timeseries(
                    expressions=retry_exprs,
                    params=params,
                    as_dataframe=as_dataframe,
                    path=path,
                    max_retry=max_retry - 1,
                    show_progress=show_progress,
                    **kwargs,
                )
                downloaded_data.extend(retried_output)
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
        start_date: str = "1990-01-01",
        end_date: str = datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        as_dataframe: bool = True,
        jpmaqs_formatting: bool = False,
        path: Optional[str] = None,
        show_progress: bool = False,
        calender: str = "CAL_WEEKDAYS",
        frequency: str = "FREQ_DAY",
        conversion: str = "CONV_LASTBUS_ABS",
        nan_treatment: str = "NA_NOTHING",
        **kwargs,
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
        :param path <Optional[str]>: Path to save the data to. Defaults to None.
        :param jpmaqs_formatting <bool>: Whether to format the data in the JPMaQS format.
            Only used if ``path`` is provided. Defaults to False.
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

        expressions = sorted(expressions)

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
        dwnld_start = time.time()
        if self.heartbeat(raise_error=True):
            print(f"Timestamp (UTC): {UTCNOW()}")

        print(
            f"Downloading {len(expressions)} expressions, from {start_date} to {end_date}.\n"
            "Downloading data from DataQuery:"
        )

        downloaded_data: Union[List[Dict], List[str]] = self._get_timeseries(
            expressions=expressions,
            params=params_dict,
            as_dataframe=as_dataframe,
            path=path,
            show_progress=show_progress,
            jpmaqs_formatting=jpmaqs_formatting,
            **kwargs,
        )
        dwnld_end = time.time()
        print(
            f"Download done.\n"
            f"Timestamp (UTC): {UTCNOW()}.\n"
            "Download took "
            f"{(dwnld_end - dwnld_start) / 60:.0f}mins {(dwnld_end - dwnld_start) % 60:.1f}s."
        )
        if path:
            if jpmaqs_formatting:
                assert all(isinstance(f, dict) for f in downloaded_data)
                exprs = list(set([d["expression"] for d in downloaded_data]))
                print(f"Data saved to {path}.")
                print(f"Downloaded {len(exprs)} / {len(expressions)} expressions.")

                return downloaded_data

            assert all(isinstance(f, str) for f in downloaded_data)
            print(
                f"Downloaded {len(downloaded_data)} / {len(expressions)} expressions."
            )
            print(f"Data saved to {path}.")
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
            " - ".join(
                (
                    expr["attributes"][0]["expression"],
                    expr["attributes"][0].get("message", mismm),
                )
            )
            for expr in downloaded_data
            if expr["attributes"][0]["time-series"] is None
        ]

        if len(missing_exprs) > 0:
            emsg = "\n\t".join(missing_exprs)
            logger.warning(
                f"Missing expressions: \n\t{emsg}\n"
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


def summary_jpmaqs_csvs(
    path: str,
    expressions_list: Optional[List[str]] = True,
    show_progress: bool = False,
) -> pd.DataFrame:
    files = glob.glob(os.path.join(path, "**", "*.csv"), recursive=True)
    # remove any file that has "jpmaqs_download_summary" in the name
    files = [
        f
        for f in files
        if not os.path.basename(f).startswith("jpmaqs_download_summary_")
    ]
    summary = {}
    for file in tqdm(files, desc="Verifying files", disable=not show_progress):
        ticker = os.path.basename(file).split(".")[0]
        df = pd.read_csv(file, parse_dates=["real_date"])
        metrics = list(set(df.columns) - {"real_date"})
        summary[ticker] = {
            "path": file,
            "start_date": df["real_date"].min().strftime("%Y-%m-%d"),
            "end_date": df["real_date"].max().strftime("%Y-%m-%d"),
            "metrics": metrics,
            "n_cols": df.shape[1],
            "expressions": construct_jpmaqs_expressions(ticker, metrics),
        }
    if expressions_list:
        found_exprs = list(
            set([expr for t in summary for expr in summary[t]["expressions"]])
        )

    missing_exprs = list(set(expressions_list) - set(found_exprs))
    if len(missing_exprs) > 0:
        for i in range(0, (min(len(missing_exprs), 25))):
            print(f"\tExpression missing from downloaded data: {missing_exprs[i]}")

        if len(missing_exprs) > i + 1:
            print(f"\t\t... (truncated {len(missing_exprs) - i -1} warnings)")
            print(f"\tTotal missing expressions: {len(missing_exprs)}")

    summary_df = (
        pd.DataFrame(summary).T.reset_index().rename(columns={"index": "ticker"})
    )
    return summary_df


def download_all_jpmaqs_to_disk(
    client_id: str,
    client_secret: str,
    proxy: Optional[Dict] = None,
    path="./data",
    show_progress: bool = False,
    jpmaqs_formatting: bool = True,
    test_expressions: Optional[List[str]] = None,
    overwrite: bool = True,
    retry_missing: bool = True,
):
    """
    Download all JPMaQS data to disk.

    :param client_id <str>: Client ID for the DataQuery API.
    :param client_secret <str>: Client secret for the DataQuery API.
    :param proxy <Optional[Dict]>: Proxy settings for the request.
    :param path <str>: Path to save the data to.
    :param start_date <str>: Start date of data to download.
    :param end_date <str>: End date of data to download.
    :param show_progress <bool>: Whether to show a progress bar for the download.
    :param jpmaqs_formatting <bool>: Whether to format the data in the JPMaQS format.
    :param test_expressions <Optional[List[str]]>: List of expressions to download.
    :param overwrite <bool>: Whether to overwrite existing files.
    """
    if not isinstance(path, str):
        raise ValueError("`path` must be a string.")

    path = os.path.join(os.path.expanduser(path), "JPMaQSDATA").replace("\\", "/")
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    else:
        if overwrite:
            shutil.rmtree(path)
            os.makedirs(path, exist_ok=True)

    data: List[Dict[str, str]] = []  # [{expression:file}, {expression:file}, ...]
    expressions = []
    with DQInterface(
        client_id=client_id,
        client_secret=client_secret,
        proxy=proxy,
        batch_size=8,
    ) as dq:
        assert dq.heartbeat(), "DataQuery API Heartbeat failed."
        if not test_expressions:
            tickers = dq.get_catalogue()
            expressions = construct_jpmaqs_expressions(tickers)
        else:
            expressions = test_expressions

        data: List[Dict] = dq.download(
            expressions=expressions,
            path=path,
            show_progress=show_progress,
            jpmaqs_formatting=jpmaqs_formatting,
        )

    summary_df = Optional[pd.DataFrame]
    if jpmaqs_formatting:
        summary_df = summary_jpmaqs_csvs(
            path, expressions_list=expressions, show_progress=show_progress
        )

    if jpmaqs_formatting and ((summary_df is not None) and (len(summary_df) > 0)):
        fname = os.path.join(
            path,
            f"jpmaqs_download_summary_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
        )
        summary_df.to_csv(fname, index=False)
        print(f"Summary of downloaded data saved to {fname}")

        saved_expressions = list(
            set([_ for elem in summary_df["expressions"].tolist() for _ in elem])
        )
        missing_exprs = list(set(expressions) - set(saved_expressions))
        # retry missing one more time without overwriting
        if len(missing_exprs) > 0 and retry_missing:
            print(f"Retrying {len(missing_exprs)} missing expressions.")
            download_all_jpmaqs_to_disk(
                client_id=client_id,
                client_secret=client_secret,
                proxy=proxy,
                path=os.path.dirname(path),
                show_progress=show_progress,
                jpmaqs_formatting=jpmaqs_formatting,
                test_expressions=missing_exprs,
                overwrite=False,
                retry_missing=False,
            )

    else:
        wmax = 0
        for dx in tqdm(data, desc="Verifying files"):
            if not os.path.exists(dx["file"]):
                wmax += 1
                if wmax < 25:
                    print(f"File not found: {dx['file']}")

        if wmax >= 25:
            print(f"... (truncated {wmax - 25} warnings)")
            print(f"Total missing files: {wmax}")


# CLI and example usage


def example_usage(
    client_id: str,
    client_secret: str,
    proxy: Optional[Dict] = None,
    test_path=None,
    show_progress: bool = False,
    jpmaqs_formatting: bool = True,
):
    """
    Example usage of the DQInterface class.
    Click "[source]" to see the code.
    """

    cids = ["USD", "EUR", "GBP", "AUD"]
    xcats = ["EQXR_NSA", "FXXR_NSA", "EQXR_VT10", "EXALLOPENNESS_NSA_1YMA"]
    tickers = [f"{cid}_{xcat}" for cid in cids for xcat in xcats]
    expressions = construct_jpmaqs_expressions(tickers)

    download_all_jpmaqs_to_disk(
        client_id=client_id,
        client_secret=client_secret,
        proxy=proxy,
        path=test_path,
        test_expressions=expressions,
        show_progress=show_progress,
        jpmaqs_formatting=jpmaqs_formatting,
    )

    print(
        "\t" + "--" * 20,
        "\n\tNOTE: USD_FXXR_NSA is not a valid expression,",
        "\n\tand is only used to demonstrate error handling in example usage.",
        "\n\t" + "--" * 20,
    )


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
            print(
                "Authentication + Heartbeat took"
                f" {end - start - API_DELAY_PARAM:.2f} seconds."
            )


def get_credentials(file: str) -> Dict:
    """
    Get the credentials from a JSON file.

    :param file <str>: Path to the credentials JSON file.
    :return <dict>: Dictionary containing the credentials.
    """
    try:
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
            print("Successfully loaded credentials.")
            return res
    except Exception as e:
        print(
            """
    ========== Error getting credentials ==========
    The credentials file must be a JSON file with the following format (proxy is optional):
        {
            "client_id": "your_client_id",
            "client_secret": "your_client_secret"
            "proxy": { "https": "https://your_proxy:port" }
        }
    ========== Error getting credentials ==========
        """
        )
        raise e


def cli(
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    proxy: Optional[Dict] = None,
    path: Optional[str] = None,
    test_path: Optional[str] = None,
    heartbeat: bool = False,
    timeseries: bool = False,
    progress: bool = False,
    test_expressions: Optional[List[str]] = None,
):
    import argparse

    parser = argparse.ArgumentParser(description="Download JPMaQS data.")

    parser.add_argument(
        "--credentials",
        type=str,
        help="Path to the credentials JSON.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--path",
        type=str,
        help="Path to save the data to. Will overwrite existing files.",
        required=False,
        default=None,
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
        "--timeseries",
        action="store_true",
        help="Save the data in the timeseries format instead of the JPMaQS format.",
        required=False,
        default=False,
    )

    parser.add_argument(
        "--progress",
        action="store_true",
        help="Whether to show a progress bar for the download.",
        required=False,
        default=False,
    )

    args = parser.parse_args()

    # both client id and secret must be provided
    if client_id is None and client_secret is None:
        if args.credentials is None:
            parser.print_help()
            return

    if bool(client_id) != bool(client_secret):
        raise ValueError("Both `client_id` and `client_secret` must be provided.")

    creds = {"client_id": client_id, "client_secret": client_secret, "proxy": proxy}

    if args.credentials is not None:
        try:
            creds = get_credentials(args.credentials)
        except Exception as e:
            print(f"Error getting credentials - {type(e).__name__} : {e}")
            return

    args.path = args.path if args.path is not None else path
    args.test_path = args.test_path if args.test_path is not None else test_path
    args.heartbeat = args.heartbeat or heartbeat
    args.timeseries = args.timeseries or timeseries
    args.progress = args.progress or progress

    if args.path is None and args.test_path is None and not args.heartbeat:
        parser.print_help()
        return

    heartbeat_test(**creds)
    if args.heartbeat:
        return

    if args.path is None:
        if args.test_path is not None:
            example_usage(
                **creds,
                test_path=args.test_path,
                show_progress=args.progress,
                jpmaqs_formatting=not args.timeseries,
            )
    else:
        download_all_jpmaqs_to_disk(
            **creds,
            path=args.path,
            show_progress=args.progress,
            jpmaqs_formatting=not args.timeseries,
            test_expressions=test_expressions,
        )


if __name__ == "__main__":
    # If setting credentials in the script, or as environment variables,
    # use the following:

    client_id = None  # "your_client_id"
    client_secret = None  # "your_client_secret"
    proxy = None  # {"https": "https://your_proxy:port"}

    cli(
        client_id=client_id,
        client_secret=client_secret,
        proxy=proxy,
        # add any other arguments you need, check cli docstring for details
        # path="path/to/save/data",
        # test_path="path/to/save/test/data",
        # heartbeat=True,
        # timeseries=False,
        # progress=True,
        # test_expressions=[
        #     "DB(JPMAQS,GBP_EQXR_NSA,value)",
        #     "DB(JPMAQS,GBP_EQXR_NSA,grading)",
        #     "DB(JPMAQS,GBP_EQXR_NSA,eop_lag)",
        #     "DB(JPMAQS,GBP_EQXR_NSA,mop_lag)",
        # ],
    )
