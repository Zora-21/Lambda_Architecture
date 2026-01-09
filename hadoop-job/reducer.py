#!/usr/bin/env python3
"""
reducer.py - Reducer avanzato (compatibile Python < 3.6)

Carica 'model.json' per pulire (filtrare) i dati.
Calcola metriche OHLC e statistiche sui dati PULITI.
Aggiunge il conteggio dei dati SCARTATI.

Emette: CHIAVE \t JSON_METRICS
"""

import sys
import json
import math

# --- Carica il modello di pulizia ---
MODEL_FILE = 'model.json'
anomaly_model = {}

try:
    with open(MODEL_FILE, 'r') as f:
        anomaly_model = json.load(f)
except FileNotFoundError:
    print("ATTENZIONE: model.json non trovato. Dati non puliti.", file=sys.stderr)
    pass
except Exception as e:
    print("Errore nel caricamento di model.json: {}".format(e), file=sys.stderr)
    pass
# --- Fine ---


# Variabili per tracciare il gruppo corrente
current_key = None
current_values = [] # Lista per (timestamp, temp)

def calculate_metrics_and_print(key, values):
    """
    Funzione helper per calcolare le metriche e stampare il JSON.
    """
    if not values:
        return
    
    try:
        # --- Filtra i dati prima dei calcoli ---
        sensor_id = key.split('-', 1)[0]
        model_params = anomaly_model.get(sensor_id)
        
        cleaned_values = []
        discarded_count = 0
        total_count = len(values) # Conteggio totale prima della pulizia

        # --- INIZIO CORREZIONE LOGICA (Versione 2) ---
        
        # Se non c'è modello, considera tutti i dati como puliti
        if not model_params:
            cleaned_values = values
            discarded_count = 0
        else:
            # Se c'è un modello, controlla la std_dev
            mean = model_params['mean']
            std_dev = model_params['std_dev']

            # Applica il filtro SOLO se std_dev è significativo (maggiore di ~0)
            # Se è 0, il filtro scarterebbe tutti i dati, quindi li teniamo.
            if std_dev > 0.0001: 
                lower_bound = mean - (3 * std_dev)
                upper_bound = mean + (3 * std_dev)
                
                for (timestamp, temp) in values:
                    if (temp < lower_bound) or (temp > upper_bound):
                        discarded_count += 1 # Scarta anomalia
                    else:
                        cleaned_values.append((timestamp, temp)) # Dato pulito
            else:
                # Se std_dev è 0.0 (o troppo piccolo), tutti i dati sono "puliti"
                cleaned_values = values
                discarded_count = 0
        
        # --- FINE CORREZIONE ---

        # Se tutti i dati sono stati scartati, invece di tornare senza output
        # emetti comunque un JSON con i conteggi (count=0) in modo che
        # i passi successivi (aggregate_stats.py) possano contabilizzare
        # correttamente i dati scartati.
        if not cleaned_values:
            # Log diagnostico
            print("Dati scartati per {}: Totali={}, Puliti=0".format(key, total_count), file=sys.stderr)

            # Costruisci un oggetto metrics minimale: nessuna metrica numerica utile
            # ma con i conteggi per consentire l'aggregazione
            metrics = {
                "open": None,
                "close": None,
                "min": None,
                "max": None,
                "count": 0,
                "total_count": total_count,
                "discarded_count": discarded_count,
                "discarded_pct": round((discarded_count / total_count) * 100, 2) if total_count > 0 else 0,
                "daily_change": None,
                "daily_change_pct": None,
                "range_pct": None,
                "volatility": None,
                "trend": 0
            }

            # Stampa comunque la riga attesa da downstream: CHIAVE \t JSON
            print("{}\t{}".format(key, json.dumps(metrics)))
            return

        # 1. Ordina i dati puliti per timestamp (crescente)
        cleaned_values.sort(key=lambda x: x[0])
        
        # Estrai solo le temperature per i calcoli
        temps = [v[1] for v in cleaned_values]
        
        # 2. Calcola metriche OHLC
        open_price = cleaned_values[0][1]
        close_price = cleaned_values[-1][1]
        min_price = min(temps)
        max_price = max(temps)
        count = len(temps)
        
        # 3. Calcola statistiche aggiuntive
        discarded_pct = (discarded_count / total_count) * 100 if total_count > 0 else 0
        
        daily_change = close_price - open_price
        daily_change_pct = (daily_change / open_price) * 100 if open_price > 0 else 0
        
        price_range = max_price - min_price
        range_pct = (price_range / open_price) * 100 if open_price > 0 else 0
        
        # Volatilità (deviazione standard dei prezzi puliti)
        mean = 0
        if count > 1:
            mean = sum(temps) / count
            variance = sum((x - mean) ** 2 for x in temps) / (count - 1)
            volatility = math.sqrt(variance)
        else:
            volatility = 0
            mean = temps[0] if temps else 0
            
        # Trend (semplice, +1 se chiude più alto, -1 se più basso)
        trend = 0
        if daily_change > 0: trend = 1
        elif daily_change < 0: trend = -1

        # --- METRICHE AVANZATE ---
        
        # 1. RSI (Relative Strength Index)
        # Calcola guadagni e perdite consecutive
        gains = []
        losses = []
        for i in range(1, len(temps)):
            delta = temps[i] - temps[i-1]
            if delta > 0:
                gains.append(delta)
            elif delta < 0:
                losses.append(abs(delta))
        
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 100 if avg_gain > 0 else 50  # Nessuna perdita = RSI 100
        
        # 2. Bollinger Bands (Mean ± 2*Volatility)
        bollinger_upper = mean + (2 * volatility)
        bollinger_lower = mean - (2 * volatility)
        
        # 3. Momentum (% change dal primo all'ultimo prezzo)
        momentum = ((close_price - open_price) / open_price) * 100 if open_price > 0 else 0

        # 4. Crea l'oggetto JSON di output
        metrics = {
            "open": round(open_price, 2),
            "close": round(close_price, 2),
            "min": round(min_price, 2),
            "max": round(max_price, 2),
            "mean": round(mean, 2),
            "count": count, # Osservazioni Pulite
            "total_count": total_count, # Osservazioni Totali
            "discarded_count": discarded_count, # Osservazioni Scartate
            "discarded_pct": discarded_pct,
            "daily_change": round(daily_change, 2),
            "daily_change_pct": round(daily_change_pct, 2),
            "range_pct": round(range_pct, 2),
            "volatility": round(volatility, 2),
            "trend": trend,
            # Nuove metriche avanzate
            "rsi": round(rsi, 2),
            "bollinger_upper": round(bollinger_upper, 2),
            "bollinger_lower": round(bollinger_lower, 2),
            "momentum": round(momentum, 2)
        }
        
        # 5. Stampa il risultato
        print("{}\t{}".format(key, json.dumps(metrics)))

    except Exception as e:
        print("Errore nel calcolo delle metriche per {}: {}".format(key, e), file=sys.stderr)


# --- Loop principale del Reducer (invariato) ---

for line in sys.stdin:
    try:
        line = line.strip()
        key, value_str = line.split('\t', 1)
        temp, timestamp = value_str.split('|')
        
        temp = float(temp)
        timestamp = int(timestamp)

        if current_key == key:
            current_values.append((timestamp, temp))
        else:
            if current_key:
                calculate_metrics_and_print(current_key, current_values)
            
            current_key = key
            current_values = [(timestamp, temp)]

    except Exception:
        pass # Ignora righe malformate

# Processa l'ultimo gruppo
if current_key:
    calculate_metrics_and_print(current_key, current_values)