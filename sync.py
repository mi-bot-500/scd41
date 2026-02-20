import requests
import csv
import os

TS_CHANNEL_ID = os.getenv('TS_CHANNEL_ID', "3256740")
TS_API_KEY = os.getenv('TS_API_KEY', "5AUV1CMWTQFP29DV")
CSV_FILE = 'data_log.csv'

def get_last_timestamp():
    if not os.path.exists(CSV_FILE) or os.stat(CSV_FILE).st_size == 0:
        return "2000-01-01T00:00:00Z"
    
    with open(CSV_FILE, 'r') as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
        if len(lines) <= 1:
            return "2000-01-01T00:00:00Z"
        
        # Берем дату из последней строки и принудительно добавляем Z, если её нет
        last_ts = lines[-1].split(',')[0].strip()
        if not last_ts.endswith('Z'):
            last_ts += 'Z'
        return last_ts

def sync():
    last_ts = get_last_timestamp()
    print(f"DEBUG: Ultimo timestamp nel CSV: {last_ts}")

    url = f"https://api.thingspeak.com/channels/{TS_CHANNEL_ID}/feeds.json"
    # Мы просим данные СТРОГО после нашей последней даты
    params = {
        'api_key': TS_API_KEY,
        'start': last_ts.replace('Z', ''), 
        'results': 8000
    }

    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        feeds = r.json().get('feeds', [])
        
        if not feeds:
            print("DEBUG: Nessun nuovo dato dal server.")
            return

        new_entries = []
        for f in feeds:
            # Сравнение с учетом Z
            current_f_ts = f['created_at']
            if current_f_ts > last_ts:
                new_entries.append([
                    current_f_ts, 
                    f.get('field1'), 
                    f.get('field2'), 
                    f.get('field3')
                ])

        if not new_entries:
            print("DEBUG: Tutti i dati ricevuti sono già presenti.")
            return

        with open(CSV_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(new_entries)
        
        print(f"DEBUG: OK! Aggiunti {len(new_entries)} record.")

    except Exception as e:
        print(f"DEBUG ERROR: {e}")

if __name__ == "__main__":
    sync()
