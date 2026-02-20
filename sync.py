import requests
import csv
import os
from datetime import datetime

# Настройки (на GitHub используй Secrets)
TS_CHANNEL_ID = os.getenv('TS_CHANNEL_ID', "3256740")
TS_API_KEY = os.getenv('TS_API_KEY', "5AUV1CMWTQFP29DV")
CSV_FILE = 'data_log.csv'

def get_last_timestamp():
    if not os.path.exists(CSV_FILE) or os.stat(CSV_FILE).st_size == 0:
        return "2000-01-01T00:00:00Z"
    with open(CSV_FILE, 'rb') as f:
        try:
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        last_line = f.readline().decode().split(',')
        return last_line[0].strip()

def sync():
    last_ts = get_last_timestamp()
    print(f"Ultimo timestamp nel CSV: {last_ts}")

    # Запрашиваем данные ПОРЦИЕЙ (до 8000 за раз, если пропустили запуски)
    url = f"https://api.thingspeak.com/channels/{TS_CHANNEL_ID}/feeds.json"
    params = {
        'api_key': TS_API_KEY,
        'start': last_ts.replace('Z', ''),
        'results': 8000
    }

    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        feeds = r.json().get('feeds', [])
        
        new_entries = [
            [f['created_at'], f.get('field1'), f.get('field2'), f.get('field3')]
            for f in feeds if f['created_at'] > last_ts
        ]

        if not new_entries:
            print("Nessun nuovo dato.")
            return

        with open(CSV_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(new_entries)
        
        print(f"Aggiunti {len(new_entries)} record.")

    except Exception as e:
        print(f"Errore: {e}")

if __name__ == "__main__":
    sync()
