import csv
import io
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone


INFLUX_URL = os.getenv(
    "INFLUX_URL", "https://eu-central-1-1.aws.cloud2.influxdata.com"
)
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ESP32")
INFLUX_MEASUREMENT = os.getenv("INFLUX_MEASUREMENT", "internet_probe_raw")
INFLUX_OVERLAP_HOURS = int(os.getenv("INFLUX_OVERLAP_HOURS", "48"))
INFLUX_FULL_RESYNC = os.getenv("INFLUX_FULL_RESYNC", "").lower() in {
    "1",
    "true",
    "yes",
}

EXPORTS = [
    (
        os.getenv("INTERNET_QUALITY_BUCKET", "internet_quality"),
        os.getenv("INTERNET_QUALITY_CSV", "internet_quality.csv"),
    ),
    (
        os.getenv("INTERNET_QUALITY_MAC_BUCKET", "internet_quality_mac"),
        os.getenv("INTERNET_QUALITY_MAC_CSV", "internet_quality_mac.csv"),
    ),
]

CSV_HEADERS = [
    "timestamp",
    "device",
    "dns_target",
    "icmp_target",
    "service_host",
    "dns_ms",
    "error_code",
    "icmp_jitter_ms",
    "icmp_loss_pct",
    "icmp_rtt_ms",
    "probe_success",
    "tcp_connect_ms",
]


def require_env(name: str, value: str | None) -> str:
    if value:
        return value
    raise RuntimeError(f"Missing required environment variable: {name}")


def row_key(row: dict[str, str]) -> tuple[str, str, str, str, str]:
    return (
        row.get("timestamp", ""),
        row.get("device", ""),
        row.get("dns_target", ""),
        row.get("icmp_target", ""),
        row.get("service_host", ""),
    )


def parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def inspect_csv_state(path: str) -> str:
    if not os.path.exists(path):
        return "missing"

    if os.stat(path).st_size == 0:
        return "empty"

    with open(path, "r", newline="") as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader, None)
        if header is None:
            return "empty"
        if header != CSV_HEADERS:
            raise ValueError(f"Unexpected CSV header in {path}: {header}")
        for row in reader:
            if row and row[0]:
                return "current"

    return "header_only"


def build_query(bucket: str, start_timestamp: str | None) -> str:
    range_start = f'time(v: "{start_timestamp}")' if start_timestamp else "0"
    return f"""from(bucket: "{bucket}")
  |> range(start: {range_start})
  |> filter(fn: (r) => r._measurement == "{INFLUX_MEASUREMENT}")
  |> map(fn: (r) => ({{ r with dns_target: if exists r.dns_target then r.dns_target else r.service_host }}))
  |> keep(columns: ["_time", "_field", "_value", "device", "dns_target", "icmp_target", "service_host"])
  |> pivot(rowKey:["_time", "device", "dns_target", "icmp_target", "service_host"], columnKey:["_field"], valueColumn:"_value")
  |> group(columns: [])
  |> sort(columns: ["_time"])"""


def run_query(bucket: str, start_timestamp: str | None) -> str:
    request = urllib.request.Request(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}",
        data=build_query(bucket, start_timestamp).encode("utf-8"),
        headers={
            "Authorization": f"Token {require_env('INFLUX_TOKEN', INFLUX_TOKEN)}",
            "Accept": "application/csv",
            "Content-Type": "application/vnd.flux",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Influx query failed for bucket {bucket}: HTTP {exc.code} {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Influx query failed for bucket {bucket}: {exc}") from exc


def normalize_bool(value: str) -> str:
    lowered = value.lower()
    if lowered == "true":
        return "1"
    if lowered == "false":
        return "0"
    return value


def map_row(row: dict[str, str]) -> dict[str, str] | None:
    timestamp = row.get("_time")
    if not timestamp:
        return None

    return {
        "timestamp": timestamp,
        "device": row.get("device", ""),
        "dns_target": row.get("dns_target", "") or row.get("service_host", ""),
        "icmp_target": row.get("icmp_target", ""),
        "service_host": row.get("service_host", ""),
        "dns_ms": row.get("dns_ms", ""),
        "error_code": row.get("error_code", ""),
        "icmp_jitter_ms": row.get("icmp_jitter_ms", ""),
        "icmp_loss_pct": row.get("icmp_loss_pct", ""),
        "icmp_rtt_ms": row.get("icmp_rtt_ms", ""),
        "probe_success": normalize_bool(row.get("probe_success", "")),
        "tcp_connect_ms": row.get("tcp_connect_ms", ""),
    }


def fetch_rows(bucket: str, start_timestamp: str | None) -> list[dict[str, str]]:
    payload = run_query(bucket, start_timestamp)
    reader = csv.DictReader(io.StringIO(payload))
    rows: list[dict[str, str]] = []

    for raw_row in reader:
        mapped = map_row(raw_row)
        if mapped is not None:
            rows.append(mapped)

    return rows


def write_full_csv(path: str, rows: list[dict[str, str]]) -> None:
    with open(path, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def append_rows(path: str, rows: list[dict[str, str]]) -> None:
    if not rows:
        return

    with open(path, "a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
        writer.writerows(rows)


def load_last_timestamp_and_keys(
    path: str,
) -> tuple[str | None, set[tuple[str, str, str, str, str]]]:
    last_timestamp: str | None = None
    seen_keys: set[tuple[str, str, str, str, str]] = set()

    with open(path, "r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            timestamp = row.get("timestamp")
            if not timestamp:
                continue

            if last_timestamp is None or timestamp > last_timestamp:
                last_timestamp = timestamp
                seen_keys = {row_key(row)}
            elif timestamp == last_timestamp:
                seen_keys.add(row_key(row))

    return last_timestamp, seen_keys


def load_recent_keys(
    path: str, start_timestamp: str
) -> set[tuple[str, str, str, str, str]]:
    seen_keys: set[tuple[str, str, str, str, str]] = set()

    with open(path, "r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            timestamp = row.get("timestamp")
            if not timestamp or timestamp < start_timestamp:
                continue
            seen_keys.add(row_key(row))

    return seen_keys


def ensure_header(path: str) -> None:
    if os.path.exists(path):
        return

    with open(path, "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
        writer.writeheader()


def sync_bucket(bucket: str, output_path: str) -> None:
    state = inspect_csv_state(output_path)

    if INFLUX_FULL_RESYNC or state in {"missing", "empty", "header_only"}:
        rows = fetch_rows(bucket, None)
        write_full_csv(output_path, rows)
        print(f"Full export wrote {len(rows)} rows from {bucket} to {output_path}")
        return

    last_timestamp, seen_keys = load_last_timestamp_and_keys(output_path)
    if last_timestamp is None:
        rows = fetch_rows(bucket, None)
        write_full_csv(output_path, rows)
        print(f"Recovered empty archive with {len(rows)} rows from {bucket} to {output_path}")
        return

    overlap_start = format_timestamp(
        parse_timestamp(last_timestamp) - timedelta(hours=INFLUX_OVERLAP_HOURS)
    )
    seen_keys = load_recent_keys(output_path, overlap_start)
    fetched_rows = fetch_rows(bucket, overlap_start)
    new_rows: list[dict[str, str]] = []

    for row in fetched_rows:
        key = row_key(row)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        new_rows.append(row)

    if not new_rows:
        print(
            f"No new rows for {bucket}; {output_path} preserved as-is. "
            f"Overlap start={overlap_start}"
        )
        return

    ensure_header(output_path)
    append_rows(output_path, new_rows)
    print(
        f"Appended {len(new_rows)} rows from {bucket} to {output_path} "
        f"(overlap start={overlap_start})"
    )


def sync() -> None:
    for bucket, output_path in EXPORTS:
        sync_bucket(bucket, output_path)


if __name__ == "__main__":
    sync()
