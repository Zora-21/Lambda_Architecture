#!/usr/bin/env python3
"""
unify_batches.py
Calcola le statistiche giornaliere aggregate, inclusa la Media Pesata (Avg Price).
"""
import sys
import json

def update_daily_stats(daily, batch):
    # Aggiorna Min/Max Assoluti
    if daily['min'] is None or batch['min'] < daily['min']:
        daily['min'] = batch['min']
    if daily['max'] is None or batch['max'] > daily['max']:
        daily['max'] = batch['max']

    # Somma dei conteggi
    daily['count'] += batch['count']
    daily['discarded_count'] += batch['discarded_count']
    daily['total_count'] += batch.get('total_count', 0)

    # Open/Close
    if daily['open'] is None:
        daily['open'] = batch['open']
    daily['close'] = batch['close']

    # Volatilità (Media Pesata)
    w_daily = daily['count']
    w_batch = batch['count']
    total_w = w_daily + w_batch

    if total_w > 0:
        daily['volatility'] = (daily['volatility'] * w_daily + batch['volatility'] * w_batch) / total_w
        # RSI (Media Pesata)
        batch_rsi = batch.get('rsi', 50)
        daily['rsi_sum'] += batch_rsi * w_batch
        # Momentum (somma pesata per media)
        batch_momentum = batch.get('momentum', 0)
        daily['momentum_sum'] += batch_momentum * w_batch

    # --- CALCOLO MEDIA (MEAN) ---
    batch_avg_price = (batch['open'] + batch['close'] + batch['min'] + batch['max']) / 4.0
    daily['weighted_sum'] += batch_avg_price * w_batch
    
    return daily

def main():
    daily_stats = {} 

    for line in sys.stdin:
        try:
            line = line.strip()
            if not line: continue
            
            parts = line.split('\t', 1)
            if len(parts) < 2: continue
            
            key = parts[0].split('-')[0] 
            metrics = json.loads(parts[1])

            if key not in daily_stats:
                daily_stats[key] = {
                    "open": None, "close": None, 
                    "min": None, "max": None,
                    "count": 0, "discarded_count": 0, "total_count": 0,
                    "volatility": 0.0,
                    "weighted_sum": 0.0,
                    "rsi_sum": 0.0,
                    "momentum_sum": 0.0
                }

            daily_stats[key] = update_daily_stats(daily_stats[key], metrics)

        except Exception:
            pass

    # Calcoli Finali
    for sensor_id, stats in daily_stats.items():
        if stats['open'] and stats['open'] > 0:
            change = stats['close'] - stats['open']
            change_pct = (change / stats['open']) * 100
            range_pct = ((stats['max'] - stats['min']) / stats['open']) * 100
        else:
            change = 0; change_pct = 0; range_pct = 0

        total_cnt = stats.get('total_count', 0)
        disc_pct = (stats['discarded_count'] / total_cnt * 100) if total_cnt > 0 else 0

        # Calcolo media finale
        mean_val = 0.0
        if stats['count'] > 0:
            mean_val = stats['weighted_sum'] / stats['count']

        # RSI media pesata
        rsi_val = stats['rsi_sum'] / stats['count'] if stats['count'] > 0 else 50
        
        # Momentum media pesata
        momentum_val = stats['momentum_sum'] / stats['count'] if stats['count'] > 0 else 0
        
        # Bollinger Bands (ricalcolate dalla media e volatilità aggregate)
        bollinger_upper = mean_val + (2 * stats['volatility'])
        bollinger_lower = mean_val - (2 * stats['volatility'])

        output = {
            "open": round(stats['open'], 2),
            "close": round(stats['close'], 2),
            "min": round(stats['min'], 2),
            "max": round(stats['max'], 2),
            "mean": round(mean_val, 2),
            "count": stats['count'],
            "discarded_count": stats['discarded_count'],
            "daily_change": round(change, 2),
            "daily_change_pct": round(change_pct, 2),
            "volatility": round(stats['volatility'], 2),
            "range_pct": round(range_pct, 2),
            "discarded_pct": round(disc_pct, 2),
            # Metriche avanzate
            "rsi": round(rsi_val, 2),
            "bollinger_upper": round(bollinger_upper, 2),
            "bollinger_lower": round(bollinger_lower, 2),
            "momentum": round(momentum_val, 2)
        }
        
        print("{}-DAILY\t{}".format(sensor_id, json.dumps(output)))

if __name__ == "__main__":
    main()