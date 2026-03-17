"""
JPMaQS File Downloader — minimal standalone client for the DataQuery File API.

Setup
-----
    export DQ_CLIENT_ID="your_client_id"
    export DQ_CLIENT_SECRET="your_client_secret"

Quick start
-----------
    from jpmaqs_file_download_example import JPMaQSFileDownloader

    dl = JPMaQSFileDownloader(out_dir="./data")

    # Browse available files
    dl.list_available_files()

    # Download today's full snapshot (all themes)
    dl.download_latest_snapshot_files()

    # Download snapshot for a specific date
    dl.download_snapshot_files_for_date("2026-03-10")

    # Download snapshots in a time window
    dl.download_snapshot_files_between("2026-03-01", "2026-03-10")

    # Download latest or historical deltas
    dl.download_latest_delta_files()
    dl.download_delta_files_between("2026-03-01", "2026-03-10")
    
    # Download all deltas ever published to get the full history of JPMaQS
    # 5-6 GB of data - takes ~10-15 minutes depending on network speed
    dl.download_all_deltas()

    # Get the ticker catalog (with dataset/theme mapping)
    catalog = dl.download_catalog()

Options: pass proxies={"https": "http://proxy:8080"} and/or verify=False
to the constructor if needed. All downloads go to `out_dir`.
"""

from __future__ import annotations

import os
import time
import datetime
import concurrent.futures as cf
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import threading

import pandas as pd
import requests
from tqdm import tqdm

DQ_TOKEN_URL = "https://authe.jpmchase.com/as/token.oauth2"
DQ_BASE_URL = "https://api-dataquery.jpmchase.com/research/dataquery-authe/api/v2"
DQ_SCOPE = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
DQ_RATE_LIMIT = 1 / 25 # 25 requests per second for general endpoints
DQ_AVAILABLE_FILES_RATE_LIMIT = 1 / 5 # 5 requests per second for available-files endpoint

WAIT_LOCK = threading.Lock()
LAST_API_CALL: Optional[datetime.datetime] = None
JPMAQS_DATASET_THEME_MAPPING = {
    "Economic surprises": "JPMAQS_ECONOMIC_SURPRISES",
    "Financial conditions": "JPMAQS_FINANCIAL_CONDITIONS",
    "Generic returns": "JPMAQS_GENERIC_RETURNS",
    "Macroeconomic balance sheets": "JPMAQS_MACROECONOMIC_BALANCE_SHEETS",
    "Macroeconomic trends": "JPMAQS_MACROECONOMIC_TRENDS",
    "Shocks and risk measures": "JPMAQS_SHOCKS_RISK_MEASURES",
    "Stylized trading factors": "JPMAQS_STYLIZED_TRADING_FACTORS",
}


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _wait_for_api_call(api_delay: float) -> None:
    global LAST_API_CALL
    with WAIT_LOCK:
        now = utc_now()
        if LAST_API_CALL is not None:
            sleep_for = api_delay - (now - LAST_API_CALL).total_seconds()
            if sleep_for > 0:
                time.sleep(sleep_for)
        LAST_API_CALL = utc_now()


@dataclass
class OAuthToken:
    client_id: str
    client_secret: str
    scope: str = DQ_SCOPE
    token_url: str = DQ_TOKEN_URL
    proxies: Optional[dict[str, str]] = None
    verify: bool = True
    _access_token: Optional[str] = field(default=None, repr=False)
    _expires_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.min.replace(
            tzinfo=datetime.timezone.utc
        ),
        repr=False,
    )

    def _refresh(self) -> None:
        r = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "aud": self.scope,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            proxies=self.proxies,
            verify=self.verify,
        )
        r.raise_for_status()
        d = r.json()
        self._access_token = d["access_token"]
        self._expires_at = utc_now() + datetime.timedelta(seconds=d["expires_in"])

    @property
    def headers(self) -> dict[str, str]:
        if self._access_token is None or utc_now() >= self._expires_at:
            self._refresh()
        return {"Authorization": f"Bearer {self._access_token}"}


class JPMaQSFileDownloader:
    def __init__(
        self,
        out_dir: str | Path = "./jpmaqs-download",
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        base_url: str = DQ_BASE_URL,
        proxies: Optional[dict[str, str]] = None,
        verify: bool = True,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = base_url.rstrip("/")
        self.proxies = proxies
        self.verify = verify
        self._auth = OAuthToken(
            client_id=client_id or os.environ["DQ_CLIENT_ID"],
            client_secret=client_secret or os.environ["DQ_CLIENT_SECRET"],
            proxies=proxies,
            verify=verify,
        )

    def _get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        _wait_for_api_call(DQ_RATE_LIMIT)
        r = requests.get(
            f"{self.base_url}{endpoint}",
            headers=self._auth.headers,
            params=params or {},
            proxies=self.proxies,
            verify=self.verify,
        )
        r.raise_for_status()
        return r.json()

    def list_groups(self) -> pd.DataFrame:
        return pd.json_normalize(self._get("/groups"), record_path=["groups"])

    def search_groups(self, keywords: str) -> pd.DataFrame:
        return pd.json_normalize(
            self._get("/groups/search", {"keywords": keywords}),
            record_path=["groups"],
        )

    def list_group_files(self, group_id: str = "JPMAQS") -> pd.DataFrame:
        return pd.json_normalize(
            self._get("/group/files", {"group-id": group_id}),
            record_path=["file-group-ids"],
        )

    def list_available_files(
        self,
        group_id: str = "JPMAQS",
        file_group_id: Optional[str] = None,
        start_date: str = "20220101",
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        params: dict = {
            "group-id": group_id,
            "start-date": start_date,
            "end-date": end_date or pd.Timestamp.utcnow().strftime("%Y%m%d"),
        }
        if file_group_id:
            params["file-group-id"] = file_group_id
        _wait_for_api_call(DQ_AVAILABLE_FILES_RATE_LIMIT)
        df = pd.json_normalize(
            self._get("/group/files/available-files", params),
            record_path=["available-files"],
        )
        return df[df["is-available"]].reset_index(drop=True)

    def download_file(
        self, file_group_id: str, file_datetime: str, filename: str
    ) -> Path:
        out = self.out_dir / filename
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists():
            return out
        _wait_for_api_call(DQ_RATE_LIMIT)
        r = requests.get(
            f"{self.base_url}/group/file/download",
            headers=self._auth.headers,
            params={"file-group-id": file_group_id, "file-datetime": file_datetime},
            proxies=self.proxies,
            verify=self.verify,
            stream=True,
            timeout=300,
        )
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return out

    def download_files(
        self,
        filenames: list[str],
        n_workers: Optional[int] = None,
    ) -> list[Path]:
        results: list[Path] = []
        failed: list[str] = []

        def _one(filename: str) -> Path:
            fgid, rest = filename.rsplit("_", 1)
            fdt = rest.split(".")[0]
            return self.download_file(fgid, fdt, filename)

        with cf.ThreadPoolExecutor(max_workers=n_workers) as pool:
            futs = {pool.submit(_one, fn): fn for fn in filenames}
            for fut in tqdm(cf.as_completed(futs), total=len(futs), desc="Downloading"):
                try:
                    results.append(fut.result())
                except Exception as e:
                    failed.append(futs[fut])
                    print(f"Failed: {futs[fut]} - {e}")

        if failed:
            print(f"{len(failed)}/{len(filenames)} files failed.")
        return results

    def download_catalog(self, add_dataset_column: bool = True) -> pd.DataFrame:
        avail = self.list_available_files(file_group_id="JPMAQS_METADATA_CATALOG")
        avail["file-datetime"] = pd.to_datetime(
            avail["file-datetime"], format="mixed", utc=True
        )
        latest = avail.sort_values("file-datetime", ascending=False).iloc[0]
        fname = latest["file-name"]
        path = self.download_file(
            "JPMAQS_METADATA_CATALOG",
            latest["file-datetime"],
            fname,
        )
        df = pd.read_parquet(path)
        if add_dataset_column:
            df["Dataset"] = (
                df["Theme"].map(JPMAQS_DATASET_THEME_MAPPING).fillna("Unknown")
            )
        return df

    def _get_latest_files(self, kind: str) -> list[str]:
        """Return filenames for the most recent business day that has files of `kind`.

        kind: "snapshot" (no _DELTA, no _METADATA) or "delta" (_DELTA only).
        """
        df = self.list_available_files()
        df["_dt"] = pd.to_datetime(df["file-datetime"], format="mixed", utc=True)

        is_delta = df["file-name"].str.contains("_DELTA")
        is_meta = df["file-name"].str.contains("_METADATA")
        df = df[is_delta if kind == "delta" else ~is_delta & ~is_meta]

        # most recent file-date that has data
        latest_date = df["_dt"].dt.normalize().max()
        day_files = df[df["_dt"].dt.normalize() == latest_date]
        print(f"Found {len(day_files)} {kind} files for {latest_date.date()}")
        return day_files["file-name"].tolist()

    def download_latest_snapshot_files(
        self, n_workers: Optional[int] = None
    ) -> list[Path]:
        return self.download_files(self._get_latest_files("snapshot"), n_workers)

    def _get_files_between(
        self,
        kind: str,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> list[str]:
        """Filter available files by kind and time range."""
        df = self.list_available_files()
        df["_dt"] = pd.to_datetime(df["file-datetime"], format="mixed", utc=True)
        start_time = pd.to_datetime(start_time, utc=True)
        end_time = pd.to_datetime(end_time, utc=True)
        df = df[(df["_dt"] >= start_time) & (df["_dt"] <= end_time)]

        is_delta = df["file-name"].str.contains("_DELTA")
        is_meta = df["file-name"].str.contains("_METADATA")
        df = df[is_delta if kind == "delta" else ~is_delta & ~is_meta]

        print(f"Found {len(df)} {kind} files between {start_time} and {end_time}.")
        return df["file-name"].tolist()

    def download_snapshot_files_between(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        n_workers: Optional[int] = None,
    ) -> list[Path]:
        return self.download_files(
            self._get_files_between("snapshot", start_time, end_time), n_workers
        )

    def download_snapshot_files_for_date(
        self, date: pd.Timestamp, n_workers: Optional[int] = None
    ) -> list[Path]:
        start = pd.to_datetime(date, utc=True).normalize()
        end = start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        return self.download_snapshot_files_between(start, end, n_workers)

    def download_latest_delta_files(
        self, n_workers: Optional[int] = None
    ) -> list[Path]:
        return self.download_files(self._get_latest_files("delta"), n_workers)

    def download_delta_files_between(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        n_workers: Optional[int] = None,
    ) -> list[Path]:
        return self.download_files(
            self._get_files_between("delta", start_time, end_time), n_workers
        )

    def download_all_deltas(self, n_workers: Optional[int] = None) -> list[Path]:
        avail = self.list_available_files()
        delta_files = avail[avail["file-name"].str.contains("_DELTA")][
            "file-name"
        ].tolist()
        existing_files = {p.name for p in self.out_dir.rglob("*")}
        to_download = [fn for fn in delta_files if fn not in existing_files]
        print(f"{len(to_download)} delta files to download.")
        return self.download_files(to_download, n_workers)


if __name__ == "__main__":
    api = JPMaQSFileDownloader(
        out_dir="./jpmaqs-download",
        client_id=os.getenv("DQ_CLIENT_ID"),
        client_secret=os.getenv("DQ_CLIENT_SECRET"),
        proxies=None,  # e.g. {"http": "http://proxy:port", "https": "http://proxy:port"}
        verify=True,  # or False to disable SSL verification (not recommended)
    )

    print("Available files:")
    print(api.list_available_files().head())
    print("\nDownloading latest catalog...")
    catalog = api.download_catalog()
    print(catalog.head())
    print("\nDownloading latest snapshot files...")
    snapshot_files = api.download_latest_snapshot_files()
    print(f"Downloaded {len(snapshot_files)} snapshot files.")
    print("\nDownloading latest delta files...")
    delta_files = api.download_latest_delta_files()
    print(f"Downloaded {len(delta_files)} delta files.")
    print("\nDownloading all delta files")
    all_delta_files = api.download_all_deltas()

    # download full-snapshot file for 2026-03-10
    print("\nDownloading snapshot files for 2026-03-10...")
    date = pd.Timestamp("2026-03-10")
    snapshot_files_for_date = api.download_snapshot_files_for_date(date)
