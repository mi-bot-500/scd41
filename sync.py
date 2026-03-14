import csv
import os
import time
from datetime import datetime, timezone

import requests

TS_CHANNEL_ID = os.getenv("TS_CHANNEL_ID")
TS_API_KEY = os.getenv("TS_API_KEY")
CSV_FILE = os.getenv("CSV_FILE", "data_log.csv")
BATCH_SIZE = 8000
REQUEST_TIMEOUT = 30
FULL_RESYNC = os.getenv("FULL_RESYNC", "").lower() in {"1", "true", "yes"}

CSV_HEADERS = [
    "timestamp",
    "co2",
    "temp",
    "hum",
    "pressure",
    "voc_index",
    "nox_index",
    "relay_on",
    "relay_state_age_s",
]
LEGACY_HEADERS = ["timestamp", "co2", "temp", "hum"]
PREVIOUS_HEADERS = [
    "timestamp",
    "co2",
    "temp",
    "hum",
    "pressure",
    "voc_index",
    "nox_index",
]


def require_env(name: str, value: str | None) -> str:
    if value:
        return value
    raise RuntimeError(f"Missing required environment variable: {name}")


def get_api_url() -> str:
    channel_id = require_env("TS_CHANNEL_ID", TS_CHANNEL_ID)
    return f"https://api.thingspeak.com/channels/{channel_id}/feeds.json"


def inspect_csv_state() -> str:
    if not os.path.exists(CSV_FILE):
        return "missing"

    with open(CSV_FILE, "r", newline="") as csv_file:
        rows = list(csv.reader(csv_file))

    if not rows:
        return "empty"

    header = rows[0]
    if header == LEGACY_HEADERS or header == PREVIOUS_HEADERS:
        return "legacy"

    if header != CSV_HEADERS:
        raise ValueError(f"Unexpected CSV header in {CSV_FILE}: {header}")

    for row in rows[1:]:
        if row and row[0]:
            return "current"

    return "header_only"
def get_last_timestamp() -> str:
    last_ts = "2000-01-01T00:00:00Z"

    if not os.path.exists(CSV_FILE) or os.stat(CSV_FILE).st_size == 0:
        return last_ts

    with open(CSV_FILE, "r", newline="") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)
        for row in reader:
            if row and row[0]:
                raw_ts = row[0].strip()
                if "T" in raw_ts:
                    last_ts = raw_ts if raw_ts.endswith("Z") else raw_ts + "Z"

    return last_ts


def fetch_batch(end_timestamp: str) -> tuple[dict, list[dict]]:
    params = {
        "api_key": require_env("TS_API_KEY", TS_API_KEY),
        "end": end_timestamp.removesuffix("Z"),
        "results": BATCH_SIZE,
    }
    response = requests.get(get_api_url(), params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    return payload.get("channel", {}), payload.get("feeds", [])


def fetch_since(start_timestamp: str) -> tuple[dict, list[dict]]:
    params = {
        "api_key": require_env("TS_API_KEY", TS_API_KEY),
        "start": start_timestamp.removesuffix("Z"),
        "results": BATCH_SIZE,
    }
    response = requests.get(get_api_url(), params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    return payload.get("channel", {}), payload.get("feeds", [])


def append_rows(rows: list[list[str]]) -> None:
    if not rows:
        return

    with open(CSV_FILE, "a", newline="") as csv_file:
        csv.writer(csv_file).writerows(rows)


def write_csv(feeds: list[dict]) -> None:
    with open(CSV_FILE, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(CSV_HEADERS)

        for feed in feeds:
            writer.writerow(
                [
                    feed["created_at"],
                    feed.get("field1"),
                    feed.get("field2"),
                    feed.get("field3"),
                    feed.get("field4"),
                    feed.get("field5"),
                    feed.get("field6"),
                    feed.get("field7"),
                    feed.get("field8"),
                ]
            )


def validate_entry_ids(feeds: list[dict]) -> list[tuple[int, int]]:
    gaps: list[tuple[int, int]] = []
    previous_id = None

    for feed in feeds:
        current_id = feed["entry_id"]
        if previous_id is not None and current_id != previous_id + 1:
            gaps.append((previous_id + 1, current_id - 1))
        previous_id = current_id

    return gaps


def catch_up_recent_entries(
    all_feeds: list[dict], seen_entry_ids: set[int]
) -> dict:
    channel_info: dict = {}

    for attempt in range(3):
        latest_timestamp = all_feeds[-1]["created_at"]

        try:
            channel_info, feeds = fetch_since(latest_timestamp)
        except Exception as exc:
            print(f"WARNING: catch-up request failed after history export: {exc}")
            return channel_info

        new_in_batch = 0
        for feed in feeds:
            entry_id = feed.get("entry_id")
            if entry_id is None or entry_id in seen_entry_ids:
                continue

            seen_entry_ids.add(entry_id)
            all_feeds.append(feed)
            new_in_batch += 1

        remote_last_entry_id = channel_info.get("last_entry_id")
        current_last_entry_id = all_feeds[-1]["entry_id"]
        print(
            f"DEBUG: Catch-up pass {attempt + 1}: added {new_in_batch} rows, "
            f"local_last_entry_id={current_last_entry_id}, "
            f"remote_last_entry_id={remote_last_entry_id}"
        )

        if remote_last_entry_id is None or current_last_entry_id >= remote_last_entry_id:
            break

        time.sleep(1)

    return channel_info


def sync_full_history() -> None:
    current_end = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    all_feeds: list[dict] = []
    seen_entry_ids: set[int] = set()
    channel_info: dict = {}

    print(f"DEBUG: Starting full sync into {CSV_FILE} from {current_end}")

    while True:
        try:
            channel_info, feeds = fetch_batch(current_end)
        except Exception as exc:
            print(f"DEBUG ERROR: failed to fetch batch ending at {current_end}: {exc}")
            return

        if not feeds:
            print("DEBUG: No more feeds returned. Reached the beginning of the channel.")
            break

        new_in_batch = 0
        oldest_created_at = feeds[0]["created_at"]

        for feed in feeds:
            entry_id = feed.get("entry_id")
            if entry_id is None or entry_id in seen_entry_ids:
                continue

            seen_entry_ids.add(entry_id)
            all_feeds.append(feed)
            new_in_batch += 1

        print(
            "DEBUG: Batch fetched: "
            f"{len(feeds)} rows, {new_in_batch} new, "
            f"oldest={feeds[0]['created_at']}, newest={feeds[-1]['created_at']}"
        )

        if len(feeds) < BATCH_SIZE:
            break

        if new_in_batch == 0:
            print("DEBUG: No progress in this batch. Stopping to avoid an infinite loop.")
            break

        current_end = oldest_created_at
        time.sleep(0.5)

    if not all_feeds:
        print("DEBUG: No data downloaded.")
        return

    all_feeds.sort(key=lambda feed: feed["entry_id"])
    latest_channel_info = catch_up_recent_entries(all_feeds, seen_entry_ids)
    all_feeds.sort(key=lambda feed: feed["entry_id"])
    write_csv(all_feeds)

    gaps = validate_entry_ids(all_feeds)
    last_entry_id = latest_channel_info.get("last_entry_id") or channel_info.get(
        "last_entry_id"
    )

    print(f"DEBUG: Full sync wrote {len(all_feeds)} rows to {CSV_FILE}")
    if last_entry_id is not None and all_feeds[-1]["entry_id"] != last_entry_id:
        print(
            "WARNING: latest exported entry_id does not match channel last_entry_id. "
            "The export may be incomplete."
        )

    if gaps:
        preview = ", ".join(f"{start}-{end}" for start, end in gaps[:5])
        print(f"WARNING: detected entry_id gaps: {preview}")
    else:
        print("DEBUG: Validation passed. entry_id sequence is continuous.")


def sync_incremental() -> None:
    cursor_ts = get_last_timestamp()
    total_added = 0

    print(f"DEBUG: Last timestamp found: {cursor_ts}")

    while True:
        try:
            channel_info, feeds = fetch_since(cursor_ts)
        except Exception as exc:
            print(f"DEBUG ERROR: {exc}")
            return

        if not feeds:
            print("DEBUG: No new data.")
            return

        new_rows = []
        latest_entry_id = None

        for feed in feeds:
            current_ts = feed["created_at"]
            if current_ts <= cursor_ts:
                continue

            new_rows.append(
                [
                    current_ts,
                    feed.get("field1"),
                    feed.get("field2"),
                    feed.get("field3"),
                    feed.get("field4"),
                    feed.get("field5"),
                    feed.get("field6"),
                    feed.get("field7"),
                    feed.get("field8"),
                ]
            )
            latest_entry_id = feed.get("entry_id")

        if not new_rows:
            print("DEBUG: No new rows after duplicate filtering.")
            return

        append_rows(new_rows)
        total_added += len(new_rows)
        cursor_ts = new_rows[-1][0]

        remote_last_entry_id = channel_info.get("last_entry_id")
        print(
            f"DEBUG: Added {len(new_rows)} rows, total {total_added}, "
            f"cursor={cursor_ts}, remote_last_entry_id={remote_last_entry_id}"
        )

        if len(feeds) < BATCH_SIZE:
            break

        if latest_entry_id is not None and remote_last_entry_id is not None:
            if latest_entry_id >= remote_last_entry_id:
                break

        time.sleep(1)

    print(f"DEBUG: Incremental sync completed, added {total_added} rows.")


def sync() -> None:
    state = inspect_csv_state()

    if FULL_RESYNC:
        print("DEBUG: FULL_RESYNC requested.")
        sync_full_history()
        return

    if state in {"missing", "empty", "header_only", "legacy"}:
        if state == "legacy":
            print("DEBUG: Legacy CSV detected. Rebuilding full history with 8 fields.")
        else:
            print(f"DEBUG: CSV state is {state}. Starting full sync.")
        sync_full_history()
        return

    sync_incremental()


if __name__ == "__main__":
    sync()
