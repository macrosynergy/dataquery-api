"""A Python wrapper for the JPMorgan DataQuery API.
This script is meant as a guide to using the JPMorgan DataQuery API.
This module does not contain any error handling, and will break if any errors are raised.
For JPMaQS specific functionality, see the macrosynergy.download module.

Macrosynergy Package : https://github.com/macrosynergy/macrosynergy

The MacroSynergy package download module : https://github.com/macrosynergy/macrosynergy/tree/develop/macrosynergy/download

"""

from math import ceil, floor
from itertools import chain
import os, io
import requests
from typing import List, Optional, Dict, Tuple
from datetime import datetime
# from tqdm import tqdm

CERT_BASE_URL: str = "https://platform.jpmorgan.com/research/dataquery/api/v2"
OAUTH_BASE_URL: str = (
    "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2"
)
OAUTH_TOKEN_URL: str = "https://authe.jpmchase.com/as/token.oauth2"
OAUTH_DQ_RESOURCE_ID: str = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
API_DELAY_PARAM: float = 0.3  # 300ms delay between requests




def fetch_data(
    client_id: str,
    client_secret: str,):
    pass
