def inspect_csv_state() -> str:
    if not os.path.exists(CSV_FILE):
        return "missing"

    if os.stat(CSV_FILE).st_size == 0:
        return "empty"

    try:
        with open(CSV_FILE, "r", encoding="utf-8-sig", newline="") as csv_file:
            rows = list(csv.reader(csv_file))
    except Exception:
        return "empty"

    if not rows:
        return "empty"

    header = rows[0]
    if header == LEGACY_HEADERS or header == PREVIOUS_HEADERS:
        return "legacy"

    if header != CSV_HEADERS:
        print(f"WARNING: Unexpected CSV header: {header}. Triggering rebuild.")
        return "empty"

    for row in rows[1:]:
        if row and row[0]:
            return "current"

    return "header_only"


def sync_incremental() -> None:
    last_timestamp = get_last_timestamp()
    overlap_start = format_timestamp(
        parse_timestamp(last_timestamp) - timedelta(seconds=TS_OVERLAP_SECONDS)
    )
    seen_signatures = load_recent_signatures(overlap_start)
    total_added = 0

    print(
        f"DEBUG: Last timestamp found: {last_timestamp}, "
        f"overlap_start={overlap_start}"
    )

    while True:
        try:
            channel_info, feeds = fetch_since(overlap_start)
        except Exception as exc:
            print(f"DEBUG ERROR: {exc}")
            return

        if not feeds:
            print("DEBUG: No new data.")
            return

        new_rows = []
        latest_entry_id = None

        for feed in feeds:
            latest_entry_id = feed.get("entry_id")
            current_ts = feed["created_at"]
            if current_ts < overlap_start:
                continue

            row = [
                current_ts,
                normalize_cell(feed.get("field1")),
                normalize_cell(feed.get("field2")),
                normalize_cell(feed.get("field3")),
                normalize_cell(feed.get("field4")),
                normalize_cell(feed.get("field5")),
                normalize_cell(feed.get("field6")),
                normalize_cell(feed.get("field7")),
                normalize_cell(feed.get("field8")),
            ]
            signature = row_signature(row)
            if signature in seen_signatures:
                continue

            seen_signatures.add(signature)
            new_rows.append(row)

        if not new_rows:
            print("DEBUG: No new rows after duplicate filtering.")
            return

        append_rows(new_rows)
        total_added += len(new_rows)
        overlap_start = new_rows[-1][0]
        seen_signatures = {row_signature(row) for row in new_rows if row[0] == overlap_start}

        remote_last_entry_id = channel_info.get("last_entry_id")
        print(
            f"DEBUG: Added {len(new_rows)} rows, total {total_added}, "
            f"cursor={overlap_start}, "
            f"remote_last_entry_id={remote_last_entry_id}"
        )

        if len(feeds) < BATCH_SIZE:
            break

        if latest_entry_id is not None and remote_last_entry_id is not None:
            if latest_entry_id >= remote_last_entry_id:
                break

        time.sleep(1)

    print(f"DEBUG: Incremental sync completed, added {total_added} rows.")
