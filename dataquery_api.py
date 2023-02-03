"""A Python wrapper for the JPMorgan DataQuery API.
This script is meant as a guide to using the JPMorgan DataQuery API.
This module does not contain any error handling, and will break if any errors are raised.
For JPMaQS specific functionality, see the macrosynergy.download module.

Macrosynergy Package : https://github.com/macrosynergy/macrosynergy

The Macrosynergy package download module : 
https://github.com/macrosynergy/macrosynergy/tree/develop/macrosynergy/download

"""

from math import ceil, floor
import requests
from typing import List, Optional, Dict
from datetime import datetime

# from tqdm import tqdm

CERT_BASE_URL: str = "https://platform.jpmorgan.com/research/dataquery/api/v2"
OAUTH_BASE_URL: str = (
    "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
)
OAUTH_TOKEN_URL: str = "https://authe.jpmchase.com/as/token.oauth2"
OAUTH_DQ_RESOURCE_ID: str = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
API_DELAY_PARAM: float = 0.3  # 300ms delay between requests. DO NOT MODIFY
EXPR_LIMIT: int = (
    20  # Maximum number of expressions per request (not per "download"). DO NOT MODIFY
)


def request_wrapper(url: str, params: dict, **kwargs) -> requests.Response:
    """
    Wrapper function for requests.request() used to make a request
    to the JPMorgan DataQuery API.
    Parameters
    :param url <str>: URL to make request to
    :param params <dict>: Parameters to pass to request
    Returns
    :return <requests.Response>: Response object
    """
    # Make request
    try:
        response = requests.request(
            url,
            params=params,
            **kwargs,
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
    # def get_access_token(client_id: str, client_secret: str) -> str:

    def __init__(
        self, client_id: str, client_secret: str, proxy: Optional[Dict] = None
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.proxy = proxy
        self.current_token: Optional[dict] = None
        self.token_data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "aud": OAUTH_DQ_RESOURCE_ID,
        }

    def get_access_token(self) -> str:
        """
        Helper function to get an access token from the DataQuery API.
        Returns
        :return <str>: Access token
        """

        def is_active(token: Optional[dict]) -> bool:
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
                created: datetime = self.current_token["created_at"]
                expires: int = self.current_token["expires_in"]
                return (datetime.now() - created).total_seconds() / 60 >= (expires - 1)

        if is_active(self.current_token):
            return self.current_token["access_token"]
        else:
            response = request_wrapper(
                url=OAUTH_TOKEN_URL,
                params=self.token_data,
                method="POST",
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
        # Make request
        return request_wrapper(
            url=url,
            params=params,
            headers={"Authorization": f"Bearer {self.get_access_token()}"},
            proxies=self.proxy,
            **kwargs,
        )

    def download(
        self, expressions: List[str], start_date: str, end_date: str
    ) -> List[Dict]:
        """
        Download data from the DataQuery API.
        Parameters
        :param expressions <list>: List of expressions to download
        :param start_date <str>: Start date of data to download
        :param end_date <str>: End date of data to download
        Returns
        :return <list>: List of dictionaries containing data
        """
        expr_batches: List[List[str]] = [
            expressions[i : min(i + EXPR_LIMIT, len(expressions))]
            for i in range(0, len(expressions), EXPR_LIMIT)
        ]
        
