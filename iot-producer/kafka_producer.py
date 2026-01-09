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
filtering_model = None
model_lock = threading.Lock()
anomaly_lock = threading.Lock()
sent_count = 0
anomaly_count = 0
last_anomaly_flush = 0


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


def load_model():
    """Carica il modello di filtering da HDFS per logging anomalie."""
    global filtering_model
    
    try:
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1{HDFS_MODEL_PATH}?op=OPEN"
        resp = requests.get(url, allow_redirects=True, timeout=10)
        if resp.status_code == 200:
            with model_lock:
                filtering_model = resp.json()
            log.info(f"🔄 Model loaded: {list(filtering_model.keys())}")
    except Exception as e:
        log.debug(f"Could not load model: {e}")


def flush_anomaly_stats():
    """Scrive le statistiche anomalie su HDFS."""
    global anomaly_count
    
    with anomaly_lock:
        if anomaly_count == 0:
            return
        local_count = anomaly_count
    
    try:
        # Leggi il totale corrente
        current_total = 0
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1{HDFS_DISCARD_STATS_PATH}?op=OPEN"
        try:
            resp = requests.get(url, allow_redirects=True, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                current_total = data.get('total', 0)
        except:
            pass
        
        # Scrivi il nuovo totale
        new_total = current_total + local_count
        create_url = f"http://{HDFS_HOST}:9870/webhdfs/v1{HDFS_DISCARD_STATS_PATH}?op=CREATE&overwrite=true"
        resp = requests.put(create_url, allow_redirects=False, timeout=10)
        if resp.status_code == 307:
            redirect_url = resp.headers['Location']
            content = json.dumps({"total": new_total})
            requests.put(redirect_url, data=content.encode('utf-8'), timeout=10)
            
            with anomaly_lock:
                anomaly_count -= local_count
            log.info(f"📊 Anomaly stats flushed: +{local_count} (total: {new_total})")
    except Exception as e:
        log.error(f"Failed to flush anomaly stats: {e}")


def is_anomaly(sensor_id, price):
    """Check if price is an anomaly using 3-sigma rule (for logging only)."""
    with model_lock:
        if not filtering_model or sensor_id not in filtering_model:
            return False
        model = filtering_model[sensor_id]
        mean = model.get('mean', price)
        std_dev = model.get('std_dev', 1000)
        if std_dev == 0:
            return False
        lower_bound = mean - 3 * std_dev
        upper_bound = mean + 3 * std_dev
        return not (lower_bound <= price <= upper_bound)


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
    global anomaly_count
    
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
            
            # Log and send
            log.info(f"[{src}] -> {sid}: ${price:.2f}")
            
            # Check for anomaly (for logging only - still send to Kafka)
            if is_anomaly(sid, price):
                with anomaly_lock:
                    anomaly_count += 1
                log.info(f"⚠️ Anomaly detected: {sid}=${price:.2f}")
            
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
    
    # Load initial model (for anomaly logging)
    load_model()
    
    # Start data source threads
    threading.Thread(target=run_binance, daemon=True).start()
    threading.Thread(target=run_coinbase, daemon=True).start()
    threading.Thread(target=run_coingecko, daemon=True).start()
    threading.Thread(target=process_queue, daemon=True).start()
    threading.Thread(target=recovery_loop, daemon=True).start()
    
    log.info("🚀 Kafka Producer Started (Multi-Source Mode)")
    
    # Main loop - refresh model periodically
    last_model_refresh = 0
    last_status_log = 0
    
    while True:
        time.sleep(1)
        now = time.time()
        
        # Refresh model every 60 seconds
        if now - last_model_refresh > 60:
            load_model()
            last_model_refresh = now
        
        # Log status every 30 seconds
        if now - last_status_log > 30:
            log.info(f"📊 Status: Sent={sent_count}, Anomalies={anomaly_count}")
            last_status_log = now
            
            # NOTE: Disattivato - il conteggio anomalie è gestito dal speed-layer-consumer
            # flush_anomaly_stats()


if __name__ == "__main__":
    main()
