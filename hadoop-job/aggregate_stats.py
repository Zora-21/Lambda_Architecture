#!/usr/bin/env python3
"""
aggregate_stats.py

Progettato per l'Architettura Incrementale.
Input (stdin): Flusso di righe JSON multiple. 
Ogni riga è l'output di un job MapReduce (micro-batch) diverso.
Esempio Input:
  {"A1-2023...": {count: 100, discarded: 5, total_count: 105...}}
  {"A1-2023...": {count: 50, discarded: 2, total_count: 52...}}

Output (stdout): Un singolo JSON aggregato con i totali di TUTTI i batch.
"""

import sys
import json

total_clean = 0
total_discarded = 0
total_processed = 0

# Legge tutte le righe provenienti da "hdfs dfs -cat .../*/part-00000"
for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue

        # L'output del reducer è nel formato: CHIAVE \t JSON
        # Esempio: "A1-2023-10-27 \t {...}"
        parts = line.split('\t', 1)
        if len(parts) < 2:
            continue
            
        json_str = parts[1]
        metrics = json.loads(json_str)
        
        # Somma i contatori parziali di questo micro-batch
        total_clean += metrics.get('count', 0) 
        total_discarded += metrics.get('discarded_count', 0)
        # Usa total_count dal reducer (che è il vero totale di righe lette)
        total_processed += metrics.get('total_count', 0)
        
    except (ValueError, json.JSONDecodeError):
        # Ignora righe che non sono JSON valido (es. log di hadoop spuri)
        pass
    except Exception:
        pass

# Se total_processed è 0 (per compatibilità con vecchi dati), calcola da count + discarded
if total_processed == 0:
    total_processed = total_clean + total_discarded

# Prepara l'output finale per la Dashboard
output = {
    "total_clean": total_clean,
    "total_discarded": total_discarded,
    "total_processed": total_processed
}

# Stampa un singolo oggetto JSON su stdout
print(json.dumps(output, indent=2))