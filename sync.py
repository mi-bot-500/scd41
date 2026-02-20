import requests
import csv
import os

# Настройки
TS_CHANNEL_ID = os.getenv('TS_CHANNEL_ID', "3256740")
TS_API_KEY = os.getenv('TS_API_KEY', "5AUV1CMWTQFP29DV")
CSV_FILE = 'data_log.csv'

def sync():
    # 1. Читаем последнюю метку времени из файла
    last_ts = "2000-01-01T00:00:00Z"
    if os.path.exists(CSV_FILE) and os.stat(CSV_FILE).st_size > 0:
        with open(CSV_FILE, 'r') as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
            if len(lines) > 1:
                # Берем первую колонку последней строки
                raw_ts = lines[-1].split(',')[0].strip()
                # Приводим к стандарту ThingSpeak (ISO 8601 с Z)
                if raw_ts and 'T' in raw_ts:
                    last_ts = raw_ts if raw_ts.endswith('Z') else raw_ts + 'Z'

    print(f"DEBUG: Ultimo timestamp trovato: {last_ts}")

    # 2. Запрос к ThingSpeak (только новые данные)
    url = f"https://api.thingspeak.com/channels/{TS_CHANNEL_ID}/feeds.json"
    params = {
        'api_key': TS_API_KEY,
        'start': last_ts.replace('Z', ''), # API не любит Z в параметре start
        'results': 8000
    }

    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        feeds = data.get('feeds', [])
        
        if not feeds:
            print("DEBUG: Nessun nuovo dato.")
            return

        # 3. Фильтруем дубликаты (строго больше последней даты)
        new_entries = []
        for f in feeds:
            current_ts = f['created_at']
            if current_ts > last_ts:
                new_entries.append([
                    current_ts,
                    f.get('field1'),
                    f.get('field2'),
                    f.get('field3')
                ])

        if not new_entries:
            print("DEBUG: Nessun record nuovo dopo la rimozione dei duplicati.")
            return

        # 4. Дописываем в файл
        with open(CSV_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(new_entries)
        
        print(f"DEBUG: Successo! Aggiunti {len(new_entries)} record.")

    except Exception as e:
        print(f"DEBUG ERROR: {e}")

if __name__ == "__main__":
    sync()
