# Getting Started

1. API Credentials

You will need to get your API credentials from [JP Morgan Developer Portal](https://developer.jpmorgan.com/).
You can download the API credentials from the portal as a JSON file and use it directly with this script,
or you can manually extract the credentials and use them as environment variables.

```json
{
  "client_id": "SOMERANDOMSTRINGHERE",
  "client_secret": "SOMEOTHERRANDOMSTRINGHERE"
}
```

2. Python 3.6+

3. Download the script:

<a href="https://raw.githubusercontent.com/macrosynergy/dataquery-api/main/dataquery_api.py" download="dataquery_api.py">Download dataquery_api.py</a>


```bash
python -m pip install pandas requests
```

4. Run the script

```bash
python dataquery_api.py --credentials credentials.json --heartbeat
```
