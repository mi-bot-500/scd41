import requests
import csv
import os

TS_CHANNEL_ID = os.getenv('TS_CHANNEL_ID', "3256740")
TS_API_KEY = os.getenv('TS_API_KEY', "5AUV1CMWTQFP29DV")
CSV_FILE = 'data_log.csv'

def sync():
    # 1. Читаем последнюю дату из файла
    last_ts = "2000-01-01T00:00:00Z"
    if os.path.exists(CSV_FILE) and os.stat(CSV_FILE).st_size > 0:
        with open(CSV_FILE, 'r') as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
            if len(lines) > 1:
                last_ts = lines[-1].split(',')[0]
                if not last_ts.endswith('Z'): last_ts += 'Z'

    # 2. Просим TS дать данные СТРОГО после этой даты
    url = f"https://api.thingspeak.com/channels/{TS_CHANNEL_ID}/feeds.json"
    params = {'api_key': TS_API_KEY, 'start': last_ts.replace('Z', ''), 'results': 8000}
    
    try:
        r = requests.get(url, params=params)
        feeds = r.json().get('feeds', [])
        
        # 3. Фильтруем: только те, что строго больше нашего последнего TS
        new_entries = [[f['created_at'], f.get('field1'), f.get('field2'), f.get('field3')] 
                       for f in feeds if f['created_at'] > last_ts]

        if not new_entries:
            print("Nessun nuovo dato.")
            return

        # 4. Дописываем
        with open(CSV_FILE, 'a', newline='') as f:
            csv.writer(f).writerows(new_entries)
        print(f"Sincronizzati {len(new_entries)} record.")

    except Exception as e:
        print(f"Errore: {e}")

if __name__ == "__main__":
    sync()
