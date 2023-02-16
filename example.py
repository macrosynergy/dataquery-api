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
