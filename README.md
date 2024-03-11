# dataquery-api

A simple Python API client for the [JPMorgan DataQuery](https://www.jpmorgan.com/solutions/cib/markets/dataquery)[API](https://developer.jpmorgan.com/products/dataquery_api).

# Getting Started

## Requirements:

1. OAuth Credentials (Client ID and Client Secret) from [JPMorgan Developer Portal](https://developer.jpmorgan.com/) with access to the DataQuery API.

2. Python 3.8+

3. An active internet connection

4. A computer with 1GB+ RAM, and atleast 5GB of free disk space for the full dataset.

## Setting up:

1. Clone the repository

```bash
git clone https://github.com/macrosynergy/dataquery-api.git
```

or simply copy the relevant files to your project directory.

2. Install the dependencies

```bash
python -m pip install -r requirements.txt
```

or simply:

```bash
python -m pip install requests pandas tqdm
```

Note: `tqdm` is only used for the progress bar, and is not essential to the functioning of the API client.

## Usage/Examples:

### Running `dataquery_api_jpmaqs.py`:

1. Save credentials to a JSON file, e.g. `credentials.json` (`proxy` is optional):

```json
{
  "client_id": "<your_client_id>",
  "client_secret": "<your_client_secret>",
  "proxy": {
    "https": "http://your_proxy:port"
  }
}
```

2. Runing the script:

```
$ python dataquery_api_jpmaqs.py [-h] [--credentials CREDENTIALS] [--path PATH] [--test-path TEST_PATH] [--heartbeat] [--timeseries] [--progress]

arguments:
-h, --help            show this help message and exit
--credentials CREDENTIALS
                      Path to the credentials JSON.
--path PATH           Path to save the data to. Will overwrite existing files.
--test-path TEST_PATH
                      Path to save the data to, for testing functionality.
--heartbeat           Test the DataQuery API heartbeat and exit.
--timeseries          Save the data in the timeseries format instead of the JPMaQS format.
--progress            Whether to show a progress bar for the download.
```

3. Test settings and connection:

Testing the connection and authentication with the DataQuery API:

```bash
python dataquery_api_jpmaqs.py --credentials credentials.json --heartbeat
```

```
>> Connection to DataQuery API
>> Authentication + Heartbeat took 2.10 seconds.
```

Testing a download with a few tickers:

```bash
python dataquery_api_jpmaqs.py --credentials credentials.json --test-path ./test-data
```

```
>> Timestamp (UTC): 2024-03-08 14:57:26.97
>> Downloading from DataQuery API:
>> Download done.
>> Timestamp (UTC): 2024-03-08 14:57:29.75.
>> Download took 0mins 3.9s.
>> Data saved to ./test-data/JPMaQSDATA.
>> Downloaded 64 / 64 expressions.
```

4. Downloading the full dataset:

Given that the full dataset is quite large, it is highly recommended to use the `--progress` flag to monitor the download progress. The data will be saved to the specified path, and will overwrite any existing files.

```bash
python dataquery_api_jpmaqs.py --credentials credentials.json --progress --path ./all-data
```

### Running `dataquery_api.py`:

You'll need to edit the `__main__` block in `dataquery_api.py` to include your own OAuth credentials, and to specify your expressions, start date, and end date. The `dataquery_api.py` script can be run directly from the command line, or imported into another script (as shown in `example.py`).

```python
# dataquery_api.py
...
# Example usage
if __name__ == "__main__":
    # Example usage
    client_id = "your_client_id"
    client_secret = "your_client_secret"
    # proxy = {'http': 'http://proxy.example.com:8080'}
    path = "path/to/save/data"

    # download any specific expressions
    with DQInterface(client_id=client_id, client_secret=client_secret) as dq:
        expressions = construct_jpmaqs_expressions(["GBP_FXXR_NSA", "USD_EQXR_NSA"])
        dq.download(
            expressions=expressions,
            start_date="2020-01-01",
            end_date="2021-01-01",
            path=path,
            show_progress=True,
            jpmaqs_formatting=True,
        )

    # or download all JPMaQS data
    download_all_jpmaqs_to_disk(
        client_id=client_id,
        client_secret=client_secret,
        # proxy=proxy,
        path=path,
        show_progress=True,
        jpmaqs_formatting=True,
    )
```

### Running `example.py`:

You'll need to replace the `client_id` and `client_secret` in `dataquery_api.py` with your own OAuth credentials. This can be using a config.yml/json file, environment variables, or hardcoding them in the file (not recommended).

```python
# example.py
from dataquery_api import DQInterface
import pandas as pd

client_id = "<your_client_id>"  # replace with your client id & secret
client_secret = "<your_client_secret>"

# initialize the DQInterface object with your client id & secret
dq = DQInterface(client_id, client_secret)

# check that you have a valid token and can access the API
assert dq.heartbeat(), "DataQuery API Heartbeat failed."

# create a list of expressions
expressions = [
    "DB(JPMAQS,USD_EQXR_VT10,value)",
    "DB(JPMAQS,AUD_EXALLOPENNESS_NSA_1YMA,value)",
]

# dates as strings in the format YYYY-MM-DD
start_date = "2020-01-25"
end_date = "2023-02-05"

# download the data
data = dq.download(
    expressions=expressions, start_date=start_date, end_date=end_date
)

print(data.head())
```
