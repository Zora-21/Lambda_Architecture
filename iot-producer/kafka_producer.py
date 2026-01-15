#!/usr/bin/env python3
"""
Kafka Producer - Multi-Source Crypto Data to Kafka

Sostituisce producer_unified.py, inviando tutti i dati a Kafka
invece che direttamente a Cassandra/HDFS.
"""

import os
import time
import json
import logging
import requests
import threading
import queue
import websocket
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Configurazione Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - KAFKA-PRODUCER - %(message)s')
log = logging.getLogger(__name__)

# Silenziamento warning librerie
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto-prices')
HDFS_HOST = os.getenv('HDFS_HOST', 'namenode')
LOCAL_BUFFER_PATH = '/buffer/kafka_buffer.jsonl'
HDFS_MODEL_PATH = '/models/model.json'
HDFS_DISCARD_STATS_PATH = '/models/discard_stats.json'

# --- API Esterne ---
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
COINBASE_API_URL = "https://api.coinbase.com/v2/prices/{}/spot"
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"

UNIFIED_MAP = {
    "btcusdt": "A1", "ethusdt": "B1", "solusdt": "C1",
    "BTC-USD": "A1", "ETH-USD": "B1", "SOL-USD": "C1",
    "bitcoin": "A1", "ethereum": "B1", "solana": "C1"
}

CRYPTO_NAMES = {
    "A1": "Bitcoin",
    "B1": "Ethereum",
    "C1": "Solana"
}

# --- Globals ---
data_queue = queue.Queue(maxsize=10000)
kafka_producer = None
sent_count = 0


def setup_kafka():
    """Setup Kafka producer with retry logic."""
    global kafka_producer
    
    max_retries = 30
    for attempt in range(max_retries):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3,
                compression_type='gzip',
                linger_ms=10
            )
            log.info("✅ Kafka Producer Connesso")
            return True
        except Exception as e:
            log.warning(f"Kafka connection attempt {attempt + 1}/{max_retries}: {e}")
            time.sleep(5)
    
    log.error("❌ Failed to connect to Kafka")
    return False




def save_local_buffer(record):
    """Salva record su buffer locale se Kafka non disponibile."""
    try:
        os.makedirs(os.path.dirname(LOCAL_BUFFER_PATH), exist_ok=True)
        with open(LOCAL_BUFFER_PATH, 'a') as f:
            f.write(json.dumps(record) + '\n')
    except Exception as e:
        log.error(f"Failed to save local buffer: {e}")


def load_local_buffer():
    """Carica e pulisce buffer locale."""
    records = []
    try:
        if os.path.exists(LOCAL_BUFFER_PATH):
            with open(LOCAL_BUFFER_PATH, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
            if records:
                log.info(f"🔄 Recovered {len(records)} records from local buffer")
                os.remove(LOCAL_BUFFER_PATH)
    except Exception as e:
        log.error(f"Failed to load local buffer: {e}")
    return records


def send_to_kafka(record):
    """Invia record a Kafka con fallback su buffer locale."""
    global sent_count
    
    try:
        future = kafka_producer.send(KAFKA_TOPIC, value=record)
        future.get(timeout=10)  # Block until sent
        sent_count += 1
        return True
    except Exception as e:
        log.warning(f"Kafka send failed, buffering locally: {e}")
        save_local_buffer(record)
        return False


# --- DATA SOURCE THREADS ---

def run_binance():
    """Binance WebSocket per trades real-time."""
    last_ts_map = {}
    
    def on_msg(ws, msg):
        try:
            j = json.loads(msg)
            if 'data' not in j or j['data']['e'] != 'trade':
                return
            d = j['data']
            sid = UNIFIED_MAP.get(d['s'].lower())
            if not sid:
                return
            ts = datetime.utcfromtimestamp(d['E'] / 1000.0)
            
            # Deduplication
            if sid in last_ts_map and last_ts_map[sid] == ts:
                return
            last_ts_map[sid] = ts
            
            price = float(d['p'])
            data_queue.put({
                "sid": sid,
                "ts": ts,
                "p": price,
                "src": "Binance"
            })
        except Exception:
            pass
    
    while True:
        try:
            ws = websocket.WebSocketApp(BINANCE_WS_URL, on_message=on_msg)
            ws.on_open = lambda w: w.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@trade", "ethusdt@trade", "solusdt@trade"],
                "id": 1
            }))
            ws.run_forever()
        except Exception:
            time.sleep(5)


def run_coinbase():
    """Coinbase REST API polling."""
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]
    while True:
        for p in pairs:
            try:
                res = requests.get(COINBASE_API_URL.format(p), timeout=5)
                if res.status_code == 200:
                    data_queue.put({
                        "sid": UNIFIED_MAP.get(p),
                        "ts": datetime.utcnow(),
                        "p": float(res.json()['data']['amount']),
                        "src": "Coinbase"
                    })
            except Exception:
                pass
        time.sleep(5)


def run_coingecko():
    """CoinGecko REST API polling."""
    params = {"ids": "bitcoin,ethereum,solana", "vs_currencies": "usd"}
    while True:
        try:
            res = requests.get(COINGECKO_API_URL, params=params, timeout=10)
            if res.status_code == 200:
                for crypto, value in res.json().items():
                    data_queue.put({
                        "sid": UNIFIED_MAP.get(crypto),
                        "ts": datetime.utcnow(),
                        "p": float(value['usd']),
                        "src": "CoinGecko"
                    })
        except Exception:
            pass
        time.sleep(20)


def process_queue():
    """Processa la coda e invia a Kafka."""
    while True:
        try:
            item = data_queue.get(timeout=1)
            sid = item['sid']
            ts = item['ts']
            price = item['p']
            src = item['src']
            
            # Build Kafka message
            record = {
                "sensor_id": sid,
                "crypto_name": CRYPTO_NAMES.get(sid, "Unknown"),
                "price": price,
                "temp": price,  # Compatibility with existing consumers
                "timestamp": ts.isoformat(),
                "source": src,
                "ingestion_time": datetime.utcnow().isoformat()
            }
            
            # Log and send (filtering happens in Speed Layer and Batch Layer, not here)
            log.info(f"[{src}] -> {sid}: ${price:.2f}")
            send_to_kafka(record)
            data_queue.task_done()
            
        except queue.Empty:
            pass
        except Exception as e:
            log.error(f"Error processing queue: {e}")


def recovery_loop():
    """Riprova periodicamente a inviare dati dal buffer locale."""
    while True:
        time.sleep(30)
        records = load_local_buffer()
        if records:
            success_count = 0
            for record in records:
                if send_to_kafka(record):
                    success_count += 1
            if success_count > 0:
                log.info(f"✅ Recovered {success_count}/{len(records)} buffered records")


def main():
    log.info("🚀 Kafka Producer Starting...")
    
    # Setup Kafka
    if not setup_kafka():
        log.error("Cannot start without Kafka connection")
        return
    
    # Start data source threads
    threading.Thread(target=run_binance, daemon=True).start()
    threading.Thread(target=run_coinbase, daemon=True).start()
    threading.Thread(target=run_coingecko, daemon=True).start()
    threading.Thread(target=process_queue, daemon=True).start()
    threading.Thread(target=recovery_loop, daemon=True).start()
    
    log.info("🚀 Kafka Producer Started (Multi-Source Mode)")
    
    # Main loop - just log status periodically
    last_status_log = 0
    
    while True:
        time.sleep(1)
        now = time.time()
        
        # Log status every 30 seconds
        if now - last_status_log > 30:
            log.info(f"📊 Status: Sent={sent_count}")
            last_status_log = now


if __name__ == "__main__":
    main()
