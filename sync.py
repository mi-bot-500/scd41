import requests
import csv
import os
import time

# Берем данные из секретов GitHub
TS_CHANNEL_ID = os.getenv('TS_CHANNEL_ID')
TS_API_KEY = os.getenv('TS_API_KEY')
CSV_FILE = 'data_log.csv'

def get_last_timestamp():
    if not os.path.exists(CSV_FILE) or os.stat(CSV_FILE).st_size == 0:
        return None
    with open(CSV_FILE, 'r') as f:
        lines = f.readlines()
        return lines[-1].split(',')[0].strip() if len(lines) > 1 else None

def sync():
    last_ts = get_last_timestamp()
    url = f"https://api.thingspeak.com/channels/{TS_CHANNEL_ID}/feeds.json?api_key={TS_API_KEY}&results=8000"
    if last_ts:
        url += f"&start={last_ts.replace('Z', '')}"

    data = requests.get(url).json()
    feeds = data.get('feeds', [])
    
    if not feeds or (len(feeds) == 1 and feeds[0]['created_at'] == last_ts):
        print("Nessun nuovo dato.")
        return

    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'co2', 'temp', 'hum'])
        
        for feed in feeds:
            if feed['created_at'] == last_ts: continue
            writer.writerow([feed['created_at'], feed.get('field1'), feed.get('field2'), feed.get('field3')])
    print(f"Sincronizzati {len(feeds)} record.")

if __name__ == "__main__":
    sync()
