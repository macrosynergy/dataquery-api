# dataquery-api

A simple Python API client for the [JPMorgan DataQuery](https://www.jpmorgan.com/solutions/cib/markets/dataquery)[API](https://developer.jpmorgan.com/products/dataquery_api).

# Getting Started

## Requirements:

1. OAuth Credentials (Client ID and Client Secret) from [JPMorgan Developer Portal](https://developer.jpmorgan.com/) with access to the DataQuery API.

1. Python 3.6+

1. An active internet connection

## Setting up:

1. Clone the repository

```bash
git clone https://github.com/macrosynergy/dataquery-api.git
```

2. Install the dependencies

```bash
python -m pip install -r requirements.txt
```

## Running `example.py`:

You'll need to replace the `client_id` and `client_secret` in `dataquery_api.py` with your own OAuth credentials. This can be using a config.yml/json file, environment variables, or hardcoding them in the file (not recommended).

```python
# example.py
from dataquery_api import DQInterface
import pandas as pd

client_id = "<your_client_id>"  # replace with your client id & secret
client_secret = "<your_client_secret>"

# initialize the DQInterface object with your client id & secret
dq: DQInterface = DQInterface(client_id, client_secret)

# check that you have a valid token and can access the API
assert dq.heartbeat(), "DataQuery API Heartbeat failed."

# create a list of expressions
expressions = [
    "DB(JPMAQS,USD_EQXR_VT10,value)",
    "DB(JPMAQS,AUD_EXALLOPENNESS_NSA_1YMA,value)",
]


# dates as strings in the format YYYY-MM-DD
start_date: str = "2020-01-25"
end_date: str = "2023-02-05"

# download the data
data: pd.DataFrame() = dq.download(
    expressions=expressions, start_date=start_date, end_date=end_date
)


print(data.head())

```
