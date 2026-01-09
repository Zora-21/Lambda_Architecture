import os
import time
import json
import logging
import requests
import threading
import queue
import websocket 
import urllib3
from collections import defaultdict
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy 
from hdfs import InsecureClient

# --- Configurazione Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - PRODUCER - %(message)s')
log = logging.getLogger(__name__)

# Silenziamento warning librerie
logging.getLogger("hdfs").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.WARNING)

# --- Env ---
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', 'cassandra-seed')
CASSANDRA_KEYSPACE = 'iot_keyspace'
HDFS_HOST = os.environ.get('HDFS_HOST', 'namenode')
HDFS_PORT = 9870
HDFS_USER = 'root'

# Directory base per l'architettura incrementale
HDFS_BASE_DIR = '/iot-data'
HDFS_INCOMING_DIR = '/iot-data/incoming' # Buffer per i nuovi dati

# --- Configurazione Timing ---
AGGREGATION_WINDOW = 1.0  
HDFS_BATCH_SIZE = 500      # Aumentato per ridurre piccoli file
HDFS_FLUSH_INTERVAL = 60   # Aumentato a 60s per ridurre carico su NameNode
STATS_FLUSH_INTERVAL = 10 

# --- Configurazione Fault Tolerance ---
MAX_RETRY_ATTEMPTS = 5
INITIAL_RETRY_DELAY = 2  # secondi
LOCAL_BUFFER_PATH = '/buffer/hdfs_buffer.jsonl'  # Volume Docker persistente

aggregation_buffer = defaultdict(list)
buffer_lock = threading.Lock()

# --- API Esterne ---
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
COINBASE_API_URL = "https://api.coinbase.com/v2/prices/{}/spot"
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/simple/price"

UNIFIED_MAP = {
    "btcusdt": "A1", "ethusdt": "B1", "solusdt": "C1",
    "BTC-USD": "A1", "ETH-USD": "B1", "SOL-USD": "C1",
    "bitcoin": "A1", "ethereum": "B1", "solana": "C1"
}

# --- Globals ---
data_queue = queue.Queue(maxsize=10000) 
cassandra_session = None
hdfs_client = None
cassandra_query = None

filtering_model = None
model_lock = threading.Lock()
HDFS_MODEL_PATH = '/models/model.json'
HDFS_DISCARD_STATS_PATH = '/models/discard_stats.json'
discard_counter = 0
discard_lock = threading.Lock()

def setup_connections():
    global cassandra_session, hdfs_client, cassandra_query
    # 1. Cassandra
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=9042, load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'))
            cassandra_session = cluster.connect() 
            cassandra_query = cassandra_session.prepare(f"INSERT INTO {CASSANDRA_KEYSPACE}.sensor_data (sensor_id, timestamp, temp) VALUES (?, ?, ?)")
            log.info("✅ Cassandra Connesso")
            break
        except Exception: time.sleep(5)
    
    # 2. HDFS
    while True:
        try:
            hdfs_client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER, timeout=120)
            # Crea cartelle struttura
            for d in [HDFS_BASE_DIR, HDFS_INCOMING_DIR, '/models']:
                if not hdfs_client.status(d, strict=False): hdfs_client.makedirs(d)
            log.info("✅ HDFS Connesso")
            break
        except Exception: time.sleep(5)

def init_discard_stats():
    if not hdfs_client: return
    try:
        if not hdfs_client.status(HDFS_DISCARD_STATS_PATH, strict=False):
            with hdfs_client.write(HDFS_DISCARD_STATS_PATH, encoding='utf-8', overwrite=True) as w:
                json.dump({"total": 0}, w)
    except Exception: pass

# --- Funzioni Fault Tolerance ---

def save_local_buffer(hdfs_buffer):
    """
    Salva il buffer su disco locale persistente.
    I dati vengono aggiunti al file esistente.
    """
    try:
        os.makedirs(os.path.dirname(LOCAL_BUFFER_PATH), exist_ok=True)
        with open(LOCAL_BUFFER_PATH, 'a', encoding='utf-8') as f:
            f.writelines(hdfs_buffer)
        log.info(f"💾 Buffer locale salvato: {len(hdfs_buffer)} righe")
    except Exception as e:
        log.error(f"Errore salvataggio buffer locale: {e}")

def load_local_buffer():
    """
    Carica il buffer locale se esiste (recovery dopo crash).
    """
    if not os.path.exists(LOCAL_BUFFER_PATH):
        return []
    try:
        with open(LOCAL_BUFFER_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if lines:
            log.info(f"🔄 Recovery: trovate {len(lines)} righe nel buffer locale")
        return lines
    except Exception as e:
        log.error(f"Errore lettura buffer locale: {e}")
        return []

def clear_local_buffer():
    """
    Cancella il buffer locale dopo scrittura HDFS confermata.
    """
    try:
        if os.path.exists(LOCAL_BUFFER_PATH):
            os.remove(LOCAL_BUFFER_PATH)
    except Exception as e:
        log.error(f"Errore cancellazione buffer locale: {e}")

def write_to_hdfs_with_retry(data_lines):
    """
    Scrive su HDFS con retry e exponential backoff.
    Restituisce True se la scrittura ha avuto successo.
    """
    global hdfs_client
    
    ts_batch = int(time.time() * 1000)
    filename = f"batch_{ts_batch}.jsonl"
    full_path = f"{HDFS_INCOMING_DIR}/{filename}"
    
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            # Re-crea client HDFS se necessario
            if not hdfs_client:
                hdfs_client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER, timeout=120)
            
            with hdfs_client.write(full_path, encoding='utf-8', overwrite=True) as w:
                w.write("".join(data_lines))
            log.info(f"💾 Batch salvato in INCOMING: {filename} ({len(data_lines)} righe)")
            return True  # Successo
            
        except Exception as e:
            delay = INITIAL_RETRY_DELAY * (2 ** attempt)
            log.warning(f"⚠️ HDFS write failed (attempt {attempt+1}/{MAX_RETRY_ATTEMPTS}): {e}")
            
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                log.info(f"⏳ Retry in {delay}s...")
                time.sleep(delay)
                # Reset client per forzare reconnection
                hdfs_client = None
    
    log.error(f"❌ HDFS write failed after {MAX_RETRY_ATTEMPTS} attempts")
    return False  # Fallimento dopo tutti i tentativi

def flush_discard_stats():
    global discard_counter
    if not hdfs_client: return
    local_count = 0
    with discard_lock:
        if discard_counter == 0: return 
        local_count = discard_counter
    
    try:
        current_total = 0
        read_success = False
        if hdfs_client.status(HDFS_DISCARD_STATS_PATH, strict=False):
            try:
                with hdfs_client.read(HDFS_DISCARD_STATS_PATH, encoding='utf-8') as r:
                    content = r.read()
                    if content:
                        data = json.loads(content)
                        current_total = int(data.get("total", 0))
                        read_success = True
            except Exception: pass
        else:
            read_success = True

        if read_success:
            new_total = current_total + local_count
            with hdfs_client.write(HDFS_DISCARD_STATS_PATH, encoding='utf-8', overwrite=True) as w:
                json.dump({"total": new_total}, w)
            with discard_lock:
                discard_counter -= local_count
    except Exception as e:
        log.error(f"Errore salvataggio stats: {e}")

def cleanup_old_batches():
    """
    Mantiene solo gli ultimi 3 file batch nella cartella incoming.
    """
    if not hdfs_client: return
    try:
        files = hdfs_client.list(HDFS_INCOMING_DIR)
        # Filtra solo i file batch
        batch_files = [f for f in files if f.startswith("batch_") and f.endswith(".jsonl")]
        
        # Se ce ne sono più di 3, cancella i più vecchi
        if len(batch_files) > 3:
            # Ordina per nome (che contiene il timestamp)
            batch_files.sort()
            
            # Identifica quelli da cancellare (tutti tranne gli ultimi 3)
            files_to_delete = batch_files[:-3]
            
            for f in files_to_delete:
                try:
                    hdfs_client.delete(f"{HDFS_INCOMING_DIR}/{f}")
                    log.info(f"🗑️ Eliminato batch vecchio: {f}")
                except Exception as e:
                    log.warning(f"Impossibile eliminare {f}: {e}")
    except Exception as e:
        log.error(f"Errore cleanup batch: {e}")

def update_model():
    global filtering_model
    import tempfile
    try:
        fast_client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER, timeout=15)
        if not fast_client.status(HDFS_MODEL_PATH, strict=False): return 
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            local_path = tmp.name
        try:
            fast_client.download(HDFS_MODEL_PATH, local_path, overwrite=True)
            with open(local_path, 'r') as f:
                c = f.read()
                if c and c.strip() != '{}':
                    with model_lock: filtering_model = json.loads(c)
                    log.info(f"🔄 Modello Aggiornato: {list(filtering_model.keys())}")
        finally:
            if os.path.exists(local_path): os.remove(local_path)
    except Exception: pass

def is_clean(sid, price):
    with model_lock:
        if not filtering_model or sid not in filtering_model: return False 
        m = filtering_model[sid]['mean']
        s = filtering_model[sid]['std_dev']
        if s == 0: return True
        # Tolleranza ampia (3 sigma)
        return (m - 3*s) <= price <= (m + 3*s)

# --- THREADS ---
def run_binance():
    last_ts_map = {}
    def on_msg(ws, msg):
        try:
            j = json.loads(msg)
            if 'data' not in j or j['data']['e'] != 'trade': return
            d = j['data']
            sid = UNIFIED_MAP.get(d['s'].lower())
            if not sid: return
            ts = datetime.utcfromtimestamp(d['E']/1000.0)
            if sid in last_ts_map and last_ts_map[sid] == ts: return 
            last_ts_map[sid] = ts
            price = float(d['p'])
            data_queue.put({"sid": sid, "ts": ts, "p": price, "src": "Binance"})
        except: pass
    while True:
        try:
            ws = websocket.WebSocketApp(BINANCE_WS_URL, on_message=on_msg)
            ws.on_open = lambda w: w.send(json.dumps({"method": "SUBSCRIBE", "params": ["btcusdt@trade", "ethusdt@trade", "solusdt@trade"], "id": 1}))
            ws.run_forever()
        except: time.sleep(5)

def run_coinbase():
    pairs = ["BTC-USD", "ETH-USD", "SOL-USD"]
    while True:
        for p in pairs:
            try:
                res = requests.get(COINBASE_API_URL.format(p), timeout=5)
                if res.status_code == 200:
                    data_queue.put({"sid": UNIFIED_MAP.get(p), "ts": datetime.utcnow(), "p": float(res.json()['data']['amount']), "src": "Coinbase"})
            except: pass
        time.sleep(5)

def run_coingecko():
    params = {"ids": "bitcoin,ethereum,solana", "vs_currencies": "usd"}
    while True:
        try:
            res = requests.get(COINGECKO_API_URL, params=params, timeout=10)
            if res.status_code == 200:
                for c, v in res.json().items(): data_queue.put({"sid": UNIFIED_MAP.get(c), "ts": datetime.utcnow(), "p": float(v['usd']), "src": "CoinGecko"})
        except: pass
        time.sleep(20)

def process_queue():
    """
    Raccoglie i dati e scrive file BATCH UNIVOCI nella cartella /incoming.
    Usa buffer locale persistente per fault tolerance.
    """
    hdfs_buffer = []
    last_hdfs_flush = time.time()
    
    # --- Recovery: carica dati dal buffer locale se esistono ---
    recovered_data = load_local_buffer()
    if recovered_data:
        log.info(f"🔄 Tentativo recovery di {len(recovered_data)} righe...")
        if write_to_hdfs_with_retry(recovered_data):
            clear_local_buffer()
            cleanup_old_batches()
            log.info("✅ Recovery completato con successo")
        else:
            log.warning("⚠️ Recovery fallito, i dati verranno ritentati")
    
    while True:
        try:
            try:
                item = data_queue.get(timeout=1)
                sid, ts, price, src = item['sid'], item['ts'], item['p'], item['src']
                log.info(f"[{src}] -> {sid}: ${price}")
                
                # Speed Layer Buffer
                with buffer_lock: aggregation_buffer[sid].append(price)
                
                # Batch Layer Buffer
                hdfs_buffer.append(json.dumps({
                    "sensor_id": sid, 
                    "timestamp": ts.isoformat(), # ISO standard
                    "temp": price, 
                    "source": src
                }) + '\n')
                
                data_queue.task_done()
            except queue.Empty: pass

            # Logica di Flush: Tempo o Dimensione
            if len(hdfs_buffer) > 0 and (len(hdfs_buffer) >= HDFS_BATCH_SIZE or (time.time() - last_hdfs_flush > HDFS_FLUSH_INTERVAL)):
                
                # 1. Salva prima su buffer locale (persistenza)
                save_local_buffer(hdfs_buffer)
                
                # 2. Tenta scrittura HDFS con retry
                if write_to_hdfs_with_retry(hdfs_buffer):
                    # 3. Solo se HDFS ha successo, cancella buffer locale
                    clear_local_buffer()
                    cleanup_old_batches()
                    hdfs_buffer = []
                    last_hdfs_flush = time.time()
                else:
                    # HDFS fallito, i dati sono nel buffer locale
                    # Saranno ritentati al prossimo ciclo o al restart
                    log.warning("⚠️ Dati salvati nel buffer locale, saranno ritentati")
                    hdfs_buffer = []  # Svuota per evitare duplicati
                    last_hdfs_flush = time.time()

        except Exception as e:
            log.error(f"Errore loop process_queue: {e}")

def process_aggregates():
    global discard_counter
    last_wait_log = 0 
    while True:
        time.sleep(AGGREGATION_WINDOW)
        has_model = False
        with model_lock:
            if filtering_model: has_model = True
        
        if not has_model:
            with buffer_lock: aggregation_buffer.clear()
            if time.time() - last_wait_log > 30: 
                log.info("⏳ In attesa del modello (Calibrazione)...")
                last_wait_log = time.time()
            continue
            
        current_data = {}
        with buffer_lock:
            for k, v in aggregation_buffer.items():
                if v: current_data[k] = v[:]
            aggregation_buffer.clear()
        
        ts_now = datetime.utcnow()
        for sid, prices in current_data.items():
            avg = sum(prices) / len(prices)
            if is_clean(sid, avg):
                try: cassandra_session.execute(cassandra_query, (sid, ts_now, avg))
                except: pass
            else:
                with discard_lock: discard_counter += 1
                log.info(f"⚠️ Anomalia scartata (Speed Layer): {sid} - ${avg:.2f}")

def main():
    setup_connections()
    init_discard_stats()
    
    threading.Thread(target=run_binance, daemon=True).start()
    threading.Thread(target=run_coinbase, daemon=True).start()
    threading.Thread(target=run_coingecko, daemon=True).start()
    threading.Thread(target=process_queue, daemon=True).start()
    threading.Thread(target=process_aggregates, daemon=True).start()

    log.info("🚀 Unified Producer Avviato (Mode: Incremental)")

    last_chk = 0
    last_stats_flush = 0

    while True:
        time.sleep(1) 
        now = time.time()
        if now - last_chk > 60:
            update_model()
            last_chk = now
        if now - last_stats_flush > STATS_FLUSH_INTERVAL:
            flush_discard_stats()
            last_stats_flush = now

if __name__ == "__main__":
    main()