try:
    import concurrent.futures, logging, os, json
    from datetime import datetime as datetime, timedelta, timezone
    import time
    from typing import Dict, Generator, Iterable, List, Optional, Union, overload
    import pandas as pd, requests, requests.compat
    from tqdm import tqdm
except ImportError as e:
    print(f"Import Error: {e}")
    print("Please install the required packages in your Python environment using the following command:")
    print("\n\t python -m pip install pandas requests tqdm\n")
logger = logging.getLogger(__name__)
OAUTH_BASE_URL = "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
TIMESERIES_ENDPOINT = "/expressions/time-series"
HEARTBEAT_ENDPOINT = "/services/heartbeat"
CATALOGUE_ENDPOINT = "/group/instruments"
OAUTH_TOKEN_URL = "https://authe.jpmchase.com/as/token.oauth2"
OAUTH_DQ_RESOURCE_ID = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
API_DELAY_PARAM = 0.2
TOKEN_EXPIRY_BUFFER = 0.9
EXPR_LIMIT = 20
JPMAQS_GROUP_ID = "JPMAQS"
MAX_RETRY = 3


def form_full_url(url, params={}):
    return requests.compat.quote(
        f"{url}?{requests.compat.urlencode(params)}" if params else url, safe="%/:=&?~#+!$,;'@()*[]"
    )


def construct_jpmaqs_expressions(ticker, metrics=["value", "grading", "eop_lag", "mop_lag"]):
    if isinstance(ticker, str):
        return [f"DB(JPMAQS,{ticker},{metric})" for metric in metrics]
    return [f"DB(JPMAQS,{t},{metric})" for t in ticker for metric in metrics]


def time_series_to_df(dicts_list):
    if isinstance(dicts_list, dict):
        dicts_list = [dicts_list]
    expressions = [d["attributes"][0]["expression"] for d in dicts_list]
    return_df = pd.concat(
        [
            pd.DataFrame(dicts_list.pop()["attributes"][0]["time-series"], columns=["real_date", "value"]).assign(
                expression=expressions.pop()
            )
            for _ in range(len(dicts_list))
        ],
        axis=0,
    ).reset_index(drop=True)[["real_date", "expression", "value"]]
    return_df["real_date"] = pd.to_datetime(return_df["real_date"])
    return return_df


def request_wrapper(url, headers=None, params=None, method="get", **kwargs):
    try:
        response = requests.request(method=method, url=url, params=params, headers=headers, **kwargs)
        if response.status_code == 200:
            return response
        else:
            raise Exception(f"Request failed with status code {response.status_code}.")
    except Exception as e:
        if isinstance(e, requests.exceptions.ProxyError):
            raise Exception("Proxy error. Check your proxy settings. Exception : ", e)
        elif isinstance(e, requests.exceptions.ConnectionError):
            raise Exception("Connection error. Check your internet connection. Exception : ", e)
        else:
            raise e


class DQInterface:
    def __init__(
        self, client_id, client_secret, proxy=None, base_url=OAUTH_BASE_URL, dq_resource_id=OAUTH_DQ_RESOURCE_ID
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.proxy = proxy
        self.dq_resource_id = dq_resource_id
        self.current_token = None
        self.base_url = base_url
        self.token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "aud": self.dq_resource_id,
        }

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs): ...
    def get_access_token(self):
        def _is_active(token=None):
            if token is None:
                return False
            expires = token["created_at"] + timedelta(seconds=token["expires_in"] * TOKEN_EXPIRY_BUFFER)
            return datetime.now() < expires

        if _is_active(self.current_token):
            return self.current_token["access_token"]
        else:
            r_json = request_wrapper(
                url=OAUTH_TOKEN_URL, data=self.token_data, method="post", proxies=self.proxy
            ).json()
            self.current_token = {
                "access_token": r_json["access_token"],
                "created_at": datetime.now(),
                "expires_in": r_json["expires_in"],
            }
            return self.current_token["access_token"]

    def _request(self, url, params, **kwargs):
        return request_wrapper(
            url=url,
            params=params,
            headers={"Authorization": f"Bearer {self.get_access_token()}"},
            method="get",
            proxies=self.proxy,
            **kwargs,
        ).json()

    def heartbeat(self, raise_error=False):
        url = self.base_url + HEARTBEAT_ENDPOINT
        response = self._request(url=url, params={"data": "NO_REFERENCE_DATA"})
        result = "info" in response
        if not result and raise_error:
            raise Exception(
                f"DataQuery API Heartbeat failed. \n Response : {response} \nUser ID: {self.get_access_token()['user_id']}\nTimestamp (UTC): {datetime.now(timezone.utc).isoformat()}"
            )
        return result

    def _fetch(self, url, params, **kwargs):
        downloaded_data = []
        response = self._request(url=url, params=params, **kwargs)
        if response is None or "instruments" not in response.keys():
            if response is not None:
                if "info" in response and "code" in response["info"] and int(response["info"]["code"]) == 204:
                    raise Exception(
                        f"Content was not found for the request: {response}\nUser ID: {self.get_access_token()['user_id']}\nURL: {form_full_url(url,params)}\nTimestamp (UTC): {datetime.now(timezone.utc).isoformat()}"
                    )
            raise Exception(
                f"Invalid response from DataQuery: {response}\nUser ID: {self.get_access_token()['user_id']}\nURL: {form_full_url(url,params)}Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}"
            )
        if kwargs.pop("show_progress_catalogue", False):
            print(".", end="", flush=True)
        downloaded_data.extend(response["instruments"])
        if "links" in response.keys() and response["links"][1]["next"] is not None:
            downloaded_data.extend(self._fetch(url=self.base_url + response["links"][1]["next"], params={}, **kwargs))
        return downloaded_data

    def get_catalogue(self, group_id=JPMAQS_GROUP_ID, verbose=True, show_progress=True):
        if verbose:
            print(f"Downloading the {group_id} catalogue from DataQuery...")
        try:
            response_list = self._fetch(
                url=self.base_url + CATALOGUE_ENDPOINT,
                params={"group-id": group_id},
                show_progress_catalogue=show_progress,
            )
            if show_progress:
                print()
        except Exception as e:
            raise e
        tickers = [d["instrument-name"] for d in response_list]
        utkr_count = len(tickers)
        tkr_idx = sorted([d["item"] for d in response_list])
        if not (min(tkr_idx) == 1 and max(tkr_idx) == utkr_count and len(set(tkr_idx)) == utkr_count):
            raise ValueError("The downloaded catalogue is corrupt.")
        return tickers

    def _save_csvs(self, timeseries_list, save_to_path):
        assert os.path.exists(save_to_path), f"Path {save_to_path} does not exist."
        results = []
        while len(timeseries_list) > 0:
            ts = timeseries_list.pop(0)
            if ts["attributes"][0]["time-series"] is None:
                continue
            expr = ts["attributes"][0]["expression"]
            pth = os.path.join(save_to_path, f"{expr}.csv")
            pd.DataFrame(ts["attributes"][0]["time-series"], columns=["real_date", "value"]).dropna().to_csv(
                pth, index=False
            )
            results.append(pth)
        return results

    def _get_timeseries(
        self,
        expressions,
        params,
        as_dataframe=True,
        save_to_path=None,
        max_retry=MAX_RETRY,
        show_progress=True,
        **kwargs,
    ):
        if max_retry < 0:
            raise Exception("Maximum number of retries reached.")
        expr_batches = [
            [expressions[i : min(i + EXPR_LIMIT, len(expressions))]] for i in range(0, len(expressions), EXPR_LIMIT)
        ]
        downloaded_data = []
        failed_batches = []
        if self.heartbeat(raise_error=True):
            print(f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}")
            print("Connected to DataQuery API!")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for expr_batch in tqdm(expr_batches, desc="Requesting data", disable=not show_progress):
                current_params = params.copy()
                current_params["expressions"] = expr_batch
                curr_url = self.base_url + TIMESERIES_ENDPOINT
                futures.append(executor.submit(self._fetch, url=curr_url, params=current_params))
                time.sleep(API_DELAY_PARAM)
            for ix, future in tqdm(enumerate(futures), desc="Downloading data", disable=not show_progress):
                try:
                    result = future.result()
                    if save_to_path is not None:
                        result = self._save_csvs(result, save_to_path)
                        if not all(result):
                            raise Exception(f"Failed to save data to path `{save_to_path}` for batch {ix}.")
                    downloaded_data.extend(result)
                except Exception as e:
                    failed_batches.append(expr_batches[ix])
                    logger.error(f"Failed to download data for batch {ix} : {e}")
        if len(failed_batches) > 0:
            retry_exprs = [expr for batch in failed_batches for expr in batch]
            if max_retry > 0:
                print(f"Retrying failed expressions: {retry_exprs};", f"\nRetries left: {max_retry}")
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
        expressions,
        start_date,
        end_date,
        as_dataframe=True,
        save_to_path=None,
        show_progress=False,
        calender="CAL_WEEKDAYS",
        frequency="FREQ_DAY",
        conversion="CONV_LASTBUS_ABS",
        nan_treatment="NA_NOTHING",
    ):
        if save_to_path is not None:
            save_to_path = os.path.expanduser(save_to_path)
            os.makedirs(os.path.normpath(save_to_path), exist_ok=True)
        if end_date is None:
            end_date = datetime.now(timezone.utc).isoformat()
        params_dict = {
            "format": "JSON",
            "start-date": start_date,
            "end-date": end_date,
            "calendar": calender,
            "frequency": frequency,
            "conversion": conversion,
            "nan_treatment": nan_treatment,
            "data": "NO_REFERENCE_DATA",
        }
        downloaded_data = self._get_timeseries(
            expressions=expressions,
            params=params_dict,
            as_dataframe=as_dataframe,
            save_to_path=save_to_path,
            show_progress=show_progress,
        )
        print(f"Download done.Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}")
        if save_to_path:
            assert all(isinstance(f, str) for f in downloaded_data)
            print(f"Data saved to {save_to_path}.")
            print(f"Downloaded {len(downloaded_data)} / {len(expressions)} files.")
            result = [
                {str(os.path.basename(f)).split(".")[0]: os.path.abspath(os.path.normpath(f))} for f in downloaded_data
            ]
            logger.info(f"Data saved to {save_to_path}.")
            logger.info(f"Saved files: {result}")
            return result
        mismm = "Expression not found; No message available."
        missing_exprs = [
            (expr["attributes"][0]["expression"], expr["attributes"][0].get("message", mismm))
            for expr in downloaded_data
            if expr["attributes"][0]["time-series"] is None
        ]
        if len(missing_exprs) > 0:
            logger.warning(f"Missing expressions: {missing_exprs}")
            print(
                f"Missing expressions: {missing_exprs}\nDownloaded {len(downloaded_data)-len(missing_exprs)} / {len(expressions)} expressions."
            )
            downloaded_data = [expr for expr in downloaded_data if expr["attributes"][0]["time-series"] is not None]
        if as_dataframe:
            return time_series_to_df(downloaded_data)
        return downloaded_data


def download_all_jpmaqs_to_disk(
    client_id, client_secret, proxy=None, path="./data", show_progress=False, start_date="1990-01-01", end_date=None
):
    with DQInterface(client_id=client_id, client_secret=client_secret, proxy=proxy) as dq:
        assert dq.heartbeat(), "DataQuery API Heartbeat failed."
        tickers = dq.get_catalogue()
        expressions = construct_jpmaqs_expressions(tickers)
        data = dq.download(
            expressions=expressions,
            start_date=start_date,
            end_date=end_date,
            save_to_path=path,
            show_progress=show_progress,
        )
