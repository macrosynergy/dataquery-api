"""
JPMaQS File Downloader — minimal standalone client for the DataQuery File API.

Setup
-----
    export DQ_CLIENT_ID="your_client_id"
    export DQ_CLIENT_SECRET="your_client_secret"

Quick start
-----------
    from dataquery_file_api_jpmaqs_example import JPMaQSFileDownloader

    dl = JPMaQSFileDownloader(out_dir="./data")

    # Browse available files
    dl.list_available_files()

    # Download the latest day's full snapshot (all themes published so far;
    # re-run later in the day to pick up any published late)
    dl.download_latest_snapshot_files()

    # Download snapshot for a specific date
    dl.download_snapshot_files_for_date("2026-10-23")

    # Download snapshots in a time window
    dl.download_snapshot_files_between("2026-10-12", "2026-10-23")

    # Download latest or historical deltas
    dl.download_latest_delta_files()
    dl.download_delta_files_between("2026-10-12", "2026-10-23")

    # Download all deltas ever published to get the full history of JPMaQS
    # ~12 GB / ~16,500 files (as of 2026-10) - takes ~15 minutes depending on network speed
    dl.download_all_deltas()

    # Get the ticker catalog (with dataset/theme mapping)
    catalog = dl.download_catalog()

    # Daily notifications (latest date by default, or pass date="2026-10-23")
    missing = dl.get_missing_data_notifications()  # with reasons where given
    revisions = dl.get_revisions_notifications()
    all_notifications = dl.load_notifications()  # dict of DataFrames by title
    dl.download_notification_files()  # all notification JSONs, to keep a record

Options: pass proxies={"https": "http://proxy:8080"} and/or verify=False
to the constructor if needed. All downloads go to `out_dir`.
"""

from __future__ import annotations

import concurrent.futures as cf
import datetime
import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from tqdm import tqdm

DQ_TOKEN_URL = "https://authe.jpmchase.com/as/token.oauth2"
DQ_BASE_URL = "https://api-dataquery.jpmchase.com/research/dataquery-authe/api/v2"
DQ_FALLBACK_BASE_URL = (
    "https://api-strm-gw01.jpmchase.com/research/dataquery-authe/api/v2"
)
DQ_SCOPE = "JPMC:URI:RS-06785-DataQueryExternalApi-PROD"
DQ_RATE_LIMIT = 1 / 25  # 25 requests per second for general endpoints
DQ_AVAILABLE_FILES_RATE_LIMIT = (
    1 / 5
)  # 5 requests per second for available-files endpoint
DQ_REQUEST_TIMEOUT = 60  # seconds, for token and listing requests
DQ_DOWNLOAD_TIMEOUT = 300  # seconds without data before a file download fails
DQ_MAX_DAYS_PER_REQUEST = (
    365  # longest date range available-files accepts in one request
)
JPMAQS_EARLIEST_FILE_DATE = "20230101"
CATALOG_FILE_GROUP_ID = "JPMAQS_METADATA_CATALOG"
NOTIFICATIONS_FILE_GROUP_ID = "JPMAQS_METADATA_NOTIFICATIONS"

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
    "Power factors": "JPMAQS_POWER_FACTORS",  # in the catalog; files not yet published
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


def _resolve_base_url(
    primary: str, fallback: str, proxies: Optional[dict[str, str]], verify: bool
) -> str:
    """Return `primary` if reachable, else `fallback`; if both fail, `primary`."""
    for url in (primary, fallback):
        try:
            requests.head(url, timeout=10, proxies=proxies, verify=verify)
        except requests.exceptions.RequestException:
            continue
        if url != primary:
            print(
                f"Warning: {primary} is not reachable, falling back to {url}. "
                "Please whitelist the primary URL in your network/firewall configuration."
            )
        return url
    return primary


def _split_date_range(
    start: datetime.date, end: datetime.date, max_days: int = DQ_MAX_DAYS_PER_REQUEST
) -> list[tuple[datetime.date, datetime.date]]:
    """Split `start`..`end` (inclusive) into consecutive windows of at most `max_days`."""
    day = datetime.timedelta(days=1)
    return [
        (start + d * day, min(start + (d + max_days - 1) * day, end))
        for d in range(0, (end - start).days + 1, max_days)
    ]


def _split_filename(filename: str) -> tuple[str, str]:
    """Split "JPMAQS_X_20250501.parquet" into ("JPMAQS_X", "20250501")."""
    file_group_id, rest = filename.rsplit("_", 1)
    return file_group_id, rest.split(".")[0]


def _add_snap_date(df: pd.DataFrame) -> pd.DataFrame:
    """Add `_snap`, the snapshot date each file belongs to.

    Files published from 06:00 UTC belong to that day's snapshot, earlier ones to the
    previous day's. Date-only filenames (full snapshots) count as 06:00.
    """
    ts = df["file-name"].str.rsplit("_", n=1).str[-1].str.split(".").str[0]
    ts = ts.where(ts.str.contains("T"), ts + "T060000")
    df = df.copy()
    df["_snap"] = (
        pd.to_datetime(ts, format="%Y%m%dT%H%M%S") - pd.Timedelta(hours=6)
    ).dt.normalize()
    return df


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
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

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
            timeout=DQ_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        d = r.json()
        self._access_token = d["access_token"]
        self._expires_at = utc_now() + datetime.timedelta(seconds=d["expires_in"])

    @property
    def headers(self) -> dict[str, str]:
        # one thread refreshes; the others wait for it and reuse the new token
        with self._lock:
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
        self.base_url = _resolve_base_url(
            base_url, DQ_FALLBACK_BASE_URL, proxies, verify
        ).rstrip("/")
        self.proxies = proxies
        self.verify = verify
        self._auth = OAuthToken(
            client_id=client_id or os.environ["DQ_CLIENT_ID"],
            client_secret=client_secret or os.environ["DQ_CLIENT_SECRET"],
            proxies=proxies,
            verify=verify,
        )

    def _get(
        self, endpoint: str, params: Optional[dict] = None, retries: int = 3
    ) -> dict:
        for attempt in range(1, retries + 1):
            _wait_for_api_call(DQ_RATE_LIMIT)
            try:
                r = requests.get(
                    f"{self.base_url}{endpoint}",
                    headers=self._auth.headers,
                    params=params or {},
                    proxies=self.proxies,
                    verify=self.verify,
                    timeout=DQ_REQUEST_TIMEOUT,
                )
                r.raise_for_status()
                payload = r.json()
                break
            except requests.exceptions.RequestException as e:
                if attempt == retries:
                    raise
                print(f"GET {endpoint} failed ({e}); retrying ({attempt}/{retries})")
                time.sleep(2 ** (attempt - 1))
        # the API can report a rejected request in a 200 response body
        if payload.get("errors"):
            raise RuntimeError(
                f"DataQuery rejected GET {endpoint} with params {params}: "
                f"{payload['errors']}"
            )
        return payload

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
        start_date: str = JPMAQS_EARLIEST_FILE_DATE,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """List available files between `start_date` and `end_date` (YYYYMMDD).

        The range is clamped to JPMAQS_EARLIEST_FILE_DATE..today. Ranges longer than
        365 days are fetched as consecutive 365-day windows.
        """
        params: dict = {"group-id": group_id}
        if file_group_id:
            params["file-group-id"] = file_group_id
        # no JPMaQS files exist before the earliest file date or after today. DQ still
        # lists a catalog for every date outside that span, and requests before the
        # earliest date are slow enough to time out, so the range is clamped to it.
        today = utc_now().date()
        start = max(
            pd.Timestamp(start_date).date(),
            pd.Timestamp(JPMAQS_EARLIEST_FILE_DATE).date(),
        )
        end = min(pd.Timestamp(end_date).date(), today) if end_date else today

        records: list[dict] = []
        for from_date, to_date in _split_date_range(start, end):
            _wait_for_api_call(DQ_AVAILABLE_FILES_RATE_LIMIT)
            payload = self._get(
                "/group/files/available-files",
                {
                    **params,
                    "start-date": from_date.strftime("%Y%m%d"),
                    "end-date": to_date.strftime("%Y%m%d"),
                },
            )
            records += payload["available-files"]
        if not records:  # an empty range is valid; return an empty listing
            return pd.DataFrame(
                columns=["file-group-id", "file-name", "file-datetime", "is-available"]
            )
        df = pd.json_normalize(records)
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
            timeout=DQ_DOWNLOAD_TIMEOUT,
        )
        r.raise_for_status()
        # write to a temp file so a failed download never leaves a partial file at `out`
        tmp = out.with_name(out.name + ".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        tmp.replace(out)
        return out

    def download_files(
        self,
        filenames: list[str],
        n_workers: Optional[int] = None,
        max_retries: int = 3,
    ) -> list[Path]:
        """Download the files in `filenames` that are not already in `out_dir`.

        Returns the paths of all of `filenames` that are on disk afterwards, so a
        re-run only fetches what is still missing.
        """
        results = [
            self.out_dir / fn for fn in filenames if (self.out_dir / fn).exists()
        ]
        missing = [fn for fn in filenames if not (self.out_dir / fn).exists()]
        print(
            f"{len(filenames)} files listed, {len(results)} already downloaded, "
            f"{len(missing)} to download."
        )
        failed: list[str] = []

        def _one(filename: str) -> Path:
            return self.download_file(*_split_filename(filename), filename)

        with cf.ThreadPoolExecutor(max_workers=n_workers) as pool:
            futs = {pool.submit(_one, fn): fn for fn in missing}
            for fut in tqdm(cf.as_completed(futs), total=len(futs), desc="Downloading"):
                try:
                    results.append(fut.result())
                except Exception as e:
                    failed.append(futs[fut])
                    print(f"Failed: {futs[fut]} - {e}")

        if failed and max_retries > 0:
            print(f"{len(failed)}/{len(missing)} files failed; retrying.")
            return results + self.download_files(failed, n_workers, max_retries - 1)
        if failed:
            print(f"{len(failed)}/{len(missing)} files failed.")
        return results

    def download_catalog(self, add_dataset_column: bool = True) -> pd.DataFrame:
        # a catalog is listed for every date and each serves the current one
        avail = self.list_available_files(
            file_group_id=CATALOG_FILE_GROUP_ID,
            start_date=(utc_now() - datetime.timedelta(days=31)).strftime("%Y%m%d"),
        )
        if avail.empty:
            raise RuntimeError("No catalog files available for download.")
        avail["_dt"] = pd.to_datetime(avail["file-datetime"], format="mixed", utc=True)
        fname = avail.sort_values("_dt").iloc[-1]["file-name"]
        path = self.download_file(*_split_filename(fname), fname)
        df = pd.read_parquet(path)
        if add_dataset_column:
            df["Dataset"] = (
                df["Theme"].map(JPMAQS_DATASET_THEME_MAPPING).fillna("Unknown")
            )
            unknown = sorted(set(df.loc[df["Dataset"] == "Unknown", "Theme"]))
            if unknown:
                print(
                    f"Warning: catalog themes missing from JPMAQS_DATASET_THEME_MAPPING: "
                    f"{unknown}. Their tickers have no downloadable dataset."
                )
        return df

    def download_notification_files(
        self,
        start_date: str = JPMAQS_EARLIEST_FILE_DATE,
        end_date: Optional[str] = None,
        n_workers: Optional[int] = None,
    ) -> list[Path]:
        """Download the notification JSONs published between `start_date` and
        `end_date` (default: all of them, a few small files per day)."""
        avail = self.list_available_files(
            file_group_id=NOTIFICATIONS_FILE_GROUP_ID,
            start_date=start_date,
            end_date=end_date,
        )
        return self.download_files(avail["file-name"].tolist(), n_workers)

    def load_notifications(self, date: Optional[str] = None) -> dict[str, pd.DataFrame]:
        """Load the notifications published on `date` (UTC), downloading any missing.

        Defaults to the latest date with notifications. Returns one DataFrame per
        notification, keyed by its title: "Missing Updates", "Changed historical
        values" and "Additional information on missing updates". Column names are
        snake_case, with "(%)" as "pct".
        """
        if date is None:
            start = (utc_now() - datetime.timedelta(days=31)).strftime("%Y%m%d")
            avail = self.list_available_files(
                file_group_id=NOTIFICATIONS_FILE_GROUP_ID, start_date=start
            )
            if avail.empty:
                print("No notifications published in the last 31 days.")
                return {}
            day = avail["file-datetime"].str[:8].max()
        else:
            day = pd.Timestamp(date).strftime("%Y%m%d")
            avail = self.list_available_files(
                file_group_id=NOTIFICATIONS_FILE_GROUP_ID, start_date=day, end_date=day
            )
        names = avail.loc[avail["file-datetime"].str[:8] == day, "file-name"].tolist()
        notifications: dict[str, pd.DataFrame] = {}
        # oldest first: a notification re-issued later in the day replaces the earlier one
        for path in sorted(self.download_files(names)):
            content = json.loads(path.read_text(encoding="utf-8"))
            df = pd.json_normalize(content, record_path=["data"])
            df.columns = [
                c.replace("(%)", "pct")
                .strip()
                .replace(" ", "_")
                .replace("-", "_")
                .lower()
                for c in df.columns
            ]
            notifications[content["metadata"]["sub_title"]] = df
        print(
            f"Notifications for {day}: { {k: len(v) for k, v in notifications.items()} }"
        )
        return notifications

    def get_missing_data_notifications(
        self, date: Optional[str] = None
    ) -> pd.DataFrame:
        """Tickers not updated as expected on `date`, with the reason where given."""
        n = self.load_notifications(date)
        missing = n.get("Missing Updates", pd.DataFrame())
        info = n.get("Additional information on missing updates", pd.DataFrame())
        if missing.empty or info.empty:
            return missing if info.empty else info
        return (
            missing.merge(info, how="left", on="ticker")
            .sort_values("ticker")
            .reset_index(drop=True)
        )

    def get_revisions_notifications(self, date: Optional[str] = None) -> pd.DataFrame:
        """Tickers whose historical values changed, as notified on `date`."""
        return self.load_notifications(date).get(
            "Changed historical values", pd.DataFrame()
        )

    def _get_latest_files(self, kind: str) -> list[str]:
        """Return `kind` filenames for the latest snapshot date.

        Files of a theme published late are not listed yet; a re-run picks them up.

        kind: "snapshot" (no _DELTA, no _METADATA) or "delta" (_DELTA only).
        """
        start = (utc_now() - datetime.timedelta(days=31)).strftime("%Y%m%d")
        df = _add_snap_date(self.list_available_files(start_date=start))

        is_delta = df["file-name"].str.contains("_DELTA")
        is_meta = df["file-name"].str.contains("_METADATA")
        snaps = df[~is_delta & ~is_meta]
        if snaps.empty:
            print("No snapshot available.")
            return []
        latest_date = snaps["_snap"].max()
        # themes published in the last 31 days but not yet for the latest date
        pending = sorted(
            set(snaps["file-group-id"])
            - set(snaps.loc[snaps["_snap"] == latest_date, "file-group-id"])
        )
        if pending:
            print(
                f"Not yet published for {latest_date.date()} (re-run later): {pending}"
            )
        is_kind = is_delta if kind == "delta" else ~is_delta & ~is_meta
        day_files = df[is_kind & (df["_snap"] == latest_date)]
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
        """Filter available files by kind and time range (both ends inclusive).

        A date-only `end_time` (midnight) includes that whole day.
        """
        start_time = pd.to_datetime(start_time, utc=True)
        end_time = pd.to_datetime(end_time, utc=True)
        if end_time == end_time.normalize():
            end_time += pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        df = self.list_available_files(start_date=start_time.strftime("%Y%m%d"))
        df["_dt"] = pd.to_datetime(df["file-datetime"], format="mixed", utc=True)
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
        return self.download_files(delta_files, n_workers)


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
    print(f"{len(snapshot_files)} latest snapshot files on disk.")
    print("\nDownloading latest delta files...")
    delta_files = api.download_latest_delta_files()
    print(f"{len(delta_files)} latest delta files on disk.")
    print("\nDownloading all delta files")
    # all_delta_files = api.download_all_deltas()
    print(f"Skipped downloading all delta files - uncomment the line above to enable.")

    # download full-snapshot file for 2026-10-23
    print("\nDownloading snapshot files for 2026-10-23...")
    date = pd.Timestamp("2026-10-23")
    snapshot_files_for_date = api.download_snapshot_files_for_date(date)
