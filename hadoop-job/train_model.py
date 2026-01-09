#!/usr/bin/env python3
import sys
import json
import math
from datetime import datetime, timedelta
import statistics

def main():
    temps_by_sensor = {} 
    
    # --- MODIFICA 1: Finestra temporale più ampia (60 minuti) ---
    # Questo evita che piccoli ritardi nel batching facciano trovare 0 dati
    now = datetime.utcnow()
    time_window_ago = now - timedelta(minutes=60)
    
    sys.stderr.write("Addestramento: Window Start: {}\n".format(time_window_ago.isoformat()))

    lines_read = 0
    valid_data_count = 0

    for line in sys.stdin:
        lines_read += 1
        try:
            line = line.strip()
            if not line: continue
            
            data = json.loads(line)
            sensor_id = data.get("sensor_id")
            temp = data.get("temp")
            timestamp_str = data.get("timestamp")

            if sensor_id and temp is not None and timestamp_str:
                # Gestione robusta timestamp
                try:
                    if '.' in timestamp_str:
                        data_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        data_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
                except:
                    continue
                
                # Filtra solo dati recenti
                if data_timestamp >= time_window_ago:
                    if sensor_id not in temps_by_sensor:
                        temps_by_sensor[sensor_id] = []
                    temps_by_sensor[sensor_id].append(float(temp))
                    valid_data_count += 1

        except Exception:
            pass 

    sys.stderr.write("Righe lette: {}, Dati validi (ultimi 60m): {}\n".format(lines_read, valid_data_count))
    
    # Se non abbiamo dati validi, stampa JSON vuoto e esci
    if not temps_by_sensor:
        print("{}") 
        return

    model = {}

    for sensor_id, temps in temps_by_sensor.items():
        n = len(temps)
        
        # Richiede almeno 3 punti dati per un modello minimo
        if n > 2: 
            # --- FILTRO IQR SEMPLIFICATO ---
            temps.sort()
            q1_idx = int(n * 0.25)
            q3_idx = int(n * 0.75)
            q1 = temps[q1_idx]
            q3 = temps[q3_idx]
            iqr = q3 - q1
            
            lower = q1 - (1.5 * iqr)
            upper = q3 + (1.5 * iqr)
            
            cleaned = [t for t in temps if lower <= t <= upper]
            
            # Fallback se il filtro è troppo aggressivo
            if len(cleaned) < 2: cleaned = temps

            if len(cleaned) > 0:
                mean_val = statistics.mean(cleaned)
                std_dev_val = statistics.stdev(cleaned) if len(cleaned) > 1 else 0.0
                
                # Evita deviazione standard zero
                if std_dev_val == 0: std_dev_val = mean_val * 0.001

                model[sensor_id] = {
                    "mean": round(mean_val, 2),
                    "std_dev": round(std_dev_val, 4)
                }

    # Stampa il modello finale
    print(json.dumps(model, indent=2))

if __name__ == "__main__":
    main()