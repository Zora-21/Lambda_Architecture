import os
import time
import json
import logging
import websocket 
import threading
from datetime import datetime
from cassandra.cluster import Cluster
from hdfs import InsecureClient

# --- Impostazioni Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# --- Variabili di Connessione ---
CASSANDRA_HOST = 'cassandra-seed'
CASSANDRA_KEYSPACE = 'iot_keyspace'
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'root'
HDFS_BASE_DIR = '/iot-data'

# --- Logica API WebSocket Binance ---
BINANCE_STREAM_URL = "wss://stream.binance.com:9443/stream"

SENSOR_MAP = {
    "A1": "btcusdt",
    "B1": "ethusdt",
    "C1": "solusdt" 
}
INVERSE_SENSOR_MAP = {v: k for k, v in SENSOR_MAP.items()}
# --- Fine ---

# Variabili globali per le connessioni
cassandra_session = None
cassandra_cluster = None
hdfs_client = None
cassandra_query = None
last_data_received_time = None

# --- Variabili per il Modello e Contatori Scarti ---
filtering_model = None
model_lock = threading.Lock() 
HDFS_MODEL_PATH = '/models/model.json' 
LAST_MODEL_CHECK_TIME = 0
MODEL_CHECK_INTERVAL = 60 # Controlla HDFS per un nuovo modello ogni 60 secondi

# Contatore per gli scarti (in memoria)
discard_counter_memory = 0
discard_lock = threading.Lock() # Lock per il contatore
HDFS_DISCARD_STATS_PATH = '/models/discard_stats.json'
HDFS_ROTATE_TRIGGER_PATH = '/models/rotate_trigger'

# Timeout per operazioni HDFS
HDFS_TIMEOUT = 15  # secondi

def setup_connections():
    """Inizializza o re-inizializza le connessioni globali."""
    global cassandra_session, cassandra_cluster, hdfs_client, cassandra_query
    
    # Chiudi connessioni esistenti se ci sono
    if cassandra_session:
        cassandra_session.shutdown()
    if cassandra_cluster:
        cassandra_cluster.shutdown()

    # Connettiti a Cassandra
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=9042)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            log.info("Connesso a Cassandra!")
            cassandra_cluster = cluster
            cassandra_session = session
            cassandra_query = cassandra_session.prepare(
                "INSERT INTO sensor_data (sensor_id, timestamp, temp) VALUES (?, ?, ?)"
            )
            break
        except Exception as e:
            log.warning(f"Attesa per Cassandra... ({e})")
            time.sleep(5)

    # Connettiti a HDFS
    while True:
        try:
            client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER)
            if not client.status(HDFS_BASE_DIR, strict=False):
                 log.info(f"Creazione directory HDFS di base: {HDFS_BASE_DIR}")
                 client.makedirs(HDFS_BASE_DIR)
            log.info("Connesso a HDFS!")
            hdfs_client = client
            break
        except Exception as e:
            log.warning(f"Attesa per HDFS... ({e})")
            time.sleep(5)

def is_data_clean(sensor_id, temp, model):
    """
    Controlla se un dato è "pulito" secondo il modello 3-Sigma.
    """
    model_params = model.get(sensor_id)
    
    if not model_params:
        return True
        
    mean = model_params.get('mean')
    std_dev = model_params.get('std_dev')
    
    # Se std_dev è 0 o nullo, accetta il dato (non possiamo filtrare)
    if mean is None or std_dev is None or std_dev <= 0:
        return True
        
    lower_bound = mean - (3 * std_dev)
    upper_bound = mean + (3 * std_dev)
    
    if lower_bound <= temp <= upper_bound:
        return True
    else:
        return False

def update_filtering_model():
    """
    Scarica il modello da HDFS su un file temporaneo locale, poi lo carica.
    Più robusto di .read() per evitare timeout su stream HTTP.
    """
    global filtering_model, hdfs_client
    
    if not hdfs_client:
        log.warning("⚠️  Update Modello: HDFS client non pronto.")
        return

    import tempfile
    
    try:
        # 1. Verifica esistenza
        status = hdfs_client.status(HDFS_MODEL_PATH, strict=False)
        if not status:
            log.info("⏳ Nessun modello trovato su HDFS.")
            return

        log.info(f"📥 Trovato modello ({HDFS_MODEL_PATH})... Download in corso...")
        
        # 2. Download atomico su file temporaneo
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            local_tmp_path = tmp_file.name

        try:
            # Scarica il file da HDFS al path locale
            hdfs_client.download(HDFS_MODEL_PATH, local_tmp_path, overwrite=True)
            
            # 3. Leggi dal file locale
            with open(local_tmp_path, 'r') as f:
                content = f.read()
                
            if not content or content.strip() == '{}':
                log.warning("⚠️  Modello scaricato vuoto.")
                return 

            new_model_data = json.loads(content)
            
            # 4. Aggiorna la variabile globale
            with model_lock:
                filtering_model = new_model_data
                
            log.info(f"✅ Modello aggiornato con successo! Sensori: {list(filtering_model.keys())}")

        except Exception as e:
            log.error(f"❌ Errore durante download/parsing modello: {e}")

        finally:
            # Pulizia file temporaneo
            if os.path.exists(local_tmp_path):
                os.remove(local_tmp_path)

    except Exception as e:
        log.error(f"❌ Errore generale update modello: {e}")

def rotate_discard_counters():
    """
    Attivato dal 'rotate_trigger'. Legge lo stato, ruota i contatori,
    salva il nuovo stato e azzera il contatore in memoria.
    """
    global discard_counter_memory, hdfs_client
    
    log.info("--- Rilevato trigger di rotazione contatori ---")
    
    current_job_discards = 0
    with discard_lock:
        current_job_discards = discard_counter_memory
        discard_counter_memory = 0 
    
    old_stats = {"previous": 0, "current": 0}
    try:
        if hdfs_client.status(HDFS_DISCARD_STATS_PATH, strict=False):
            with hdfs_client.read(HDFS_DISCARD_STATS_PATH, encoding='utf-8') as reader:
                old_stats = json.load(reader)
    except Exception as e:
        log.warning(f"Impossibile leggere il vecchio file di stato degli scarti. Ricomincio da zero. {e}")

    old_previous = old_stats.get("previous", 0)
    old_current = old_stats.get("current", 0)
    
    new_stats = {
        "previous": old_previous + old_current, 
        "current": current_job_discards
    }

    try:
        with hdfs_client.write(HDFS_DISCARD_STATS_PATH, encoding='utf-8', overwrite=True) as writer:
            json.dump(new_stats, writer)
        log.info(f"Statistiche scarti aggiornate su HDFS: {new_stats}")
    except Exception as e:
        log.error(f"Impossibile scrivere il nuovo file di stato degli scarti! {e}")
        with discard_lock:
            discard_counter_memory += current_job_discards

    try:
        hdfs_client.delete(HDFS_ROTATE_TRIGGER_PATH)
        log.info("File trigger rimosso.")
    except Exception as e:
        log.error(f"Impossibile rimuovere il file trigger! {e}")


def on_message(ws, message):
    """
    Callback eseguito per OGNI messaggio ricevuto dal WebSocket.
    Modificato per gestire lo stream @trade.
    """
    global cassandra_query, hdfs_client, last_data_received_time, discard_counter_memory
    try:
        wrapper = json.loads(message)
        if 'data' not in wrapper:
            return
        
        trade_data = wrapper['data']
        
        if trade_data.get('e') != 'trade': # Controlla che sia un trade
            return

        symbol = trade_data['s'].lower() 
        sensor_id = INVERSE_SENSOR_MAP.get(symbol, "UNKNOWN")
        
        if sensor_id == "UNKNOWN":
            return 
        
        price = float(trade_data['p']) 
        timestamp_ms = trade_data['E'] 
        timestamp = datetime.utcfromtimestamp(timestamp_ms / 1000.0)

        data = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temp": price 
        }
        
        log.info(f"Dato ricevuto: {data['sensor_id']} | Prezzo: {data['temp']}")
        last_data_received_time = time.time()

        # --- Logica di Scrittura Selettiva e Conteggio Scarti ---
        is_clean = False
        current_model = None
        
        with model_lock: 
            if filtering_model:
                current_model = filtering_model
        
        if current_model:
            if is_data_clean(sensor_id, price, current_model):
                is_clean = True
            else:
                log.warning(f"DATO FILTRATO (Anomalia): {sensor_id} | Prezzo: {price}")
                is_clean = False
                with discard_lock:
                    discard_counter_memory += 1
        else:
            is_clean = False
            log.info("Dato non inviato a Cassandra (in attesa del modello).")

        # 2. Invio a HDFS (SEMPRE)
        try:
            current_date_str = data['timestamp'].strftime('%Y-%m-%d')
            HDFS_PARTITION_DIR = f"{HDFS_BASE_DIR}/date={current_date_str}"
            HDFS_FILE = f"{HDFS_PARTITION_DIR}/crypto_trades.jsonl"

            try:
                if not hdfs_client.status(HDFS_PARTITION_DIR, strict=False):
                    hdfs_client.makedirs(HDFS_PARTITION_DIR)
            except Exception as e:
                log.debug(f"Errore check dir HDFS: {e}")

            try:
                if not hdfs_client.status(HDFS_FILE, strict=False):
                    log.info(f"Creazione file di log: {HDFS_FILE}")
                    with hdfs_client.write(HDFS_FILE, encoding='utf-8', append=False) as writer:
                        writer.write("")
            except Exception as e:
                log.debug(f"Errore create file HDFS: {e}")

            data_hdfs = data.copy()
            data_hdfs['timestamp'] = data_hdfs['timestamp'].isoformat()
            json_data = json.dumps(data_hdfs) + '\n'
            
            try:
                with hdfs_client.write(HDFS_FILE, encoding='utf-8', append=True) as writer:
                    writer.write(json_data)
            except Exception as e:
                log.error(f"Errore scrittura HDFS: {e}")
                
        except Exception as e:
            log.error(f"Errore scrittura HDFS: {e}")

        # 3. Invio a Cassandra (SOLO SE PULITO)
        if is_clean:
            try:
                cassandra_session.execute(
                    cassandra_query, 
                    (data['sensor_id'], data['timestamp'], data['temp'])
                )
            except Exception as e:
                log.error(f"Errore scrittura Cassandra: {e}")
        
    except Exception as e:
        log.error(f"Errore nell'elaborazione del messaggio: {e} - Messaggio: {message}")

def on_error(ws, error):
    log.error(f"Errore WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    log.info(f"Connessione WebSocket chiusa: {close_msg}")

def model_watcher():
    """
    Thread separato che aggiorna il modello ogni 60 secondi.
    Non blocca il WebSocket principale.
    """
    global LAST_MODEL_CHECK_TIME
    
    log.info("🔄 Model Watcher avviato")
    
    while True:
        try:
            time.sleep(60)  # Controlla ogni minuto
            current_time = time.time()
            
            # Aggiorna il modello (con timeout interno)
            log.info("🔄 Controllo periodico aggiornamenti modello da HDFS...")
            update_filtering_model()
            LAST_MODEL_CHECK_TIME = current_time
            
        except Exception as e:
            log.error(f"❌ Errore nel model watcher: {type(e).__name__}: {e}")
            time.sleep(15)  # Attendi 15s prima di riprovare

def on_open(ws):
    """ 
    Callback eseguito all'apertura della connessione. 
    Modificato per usare lo stream @trade.
    """
    log.info("🔗 Connessione WebSocket aperta. Sottoscrizione ai flussi...")
    
    streams = [f"{symbol}@trade" for symbol in SENSOR_MAP.values()]
    
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))
    log.info(f"📡 Sottoscritto a: {streams}")

def main():
    global last_data_received_time, LAST_MODEL_CHECK_TIME
    log.info("🚀 Avvio del producer di dati crypto...")
    
    setup_connections()

    log.info("⏳ Fase di raccolta dati per i primi 5 minuti...")
    startup_time = time.time()
    model_attempt_start_time = startup_time + 300  # Dopo 5 minuti
    
    # Avvia il thread model watcher (separato, non blocca)
    watcher_thread = threading.Thread(target=model_watcher, daemon=True)
    watcher_thread.start()
    log.info("✅ Model Watcher thread avviato")
    
    # Inizializza il file di stato scarti se non esiste
    if hdfs_client and not hdfs_client.status(HDFS_DISCARD_STATS_PATH, strict=False):
        log.info(f"📝 Inizializzazione file di stato scarti su {HDFS_DISCARD_STATS_PATH}")
        try:
            with hdfs_client.write(HDFS_DISCARD_STATS_PATH, encoding='utf-8', overwrite=True) as writer:
                json.dump({"previous": 0, "current": 0}, writer)
        except Exception as e:
            log.error(f"❌ Impossibile inizializzare file scarti: {e}")

    try:
        while True: 
            log.info("Tentativo di connessione a WebSocket...")
            ws_app = websocket.WebSocketApp(
                BINANCE_STREAM_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            ws_thread = threading.Thread(target=ws_app.run_forever, kwargs={
                "ping_interval": 5,
                "ping_timeout": 4
            }, daemon=True)
            
            ws_thread.start()
            
            last_data_received_time = time.time()
            watchdog_timeout = 40
            
            while ws_thread.is_alive():
                time.sleep(10) # Controlla ogni 10 secondi
                
                # --- Logica Watchdog ---
                time_since_last_data = time.time() - last_data_received_time
                if time_since_last_data > watchdog_timeout:
                    log.warning(f"Watchdog: Nessun dato ricevuto in {watchdog_timeout} secondi. Forzo la chiusura...")
                    ws_app.close()
                    break 
                
                # --- Logica Model Updater E Rotazione Contatori ---
                now = time.time()
                
                if now >= model_attempt_start_time and not filtering_model:
                    if (now - LAST_MODEL_CHECK_TIME) > MODEL_CHECK_INTERVAL or LAST_MODEL_CHECK_TIME == startup_time:
                        log.info("🔄 Tentativo di caricamento modello (dopo 5 minuti di raccolta dati)...")
                        update_filtering_model()
                        LAST_MODEL_CHECK_TIME = now
                
                # Controlla se il cron job ha lasciato il file trigger
                if hdfs_client and hdfs_client.status(HDFS_ROTATE_TRIGGER_PATH, strict=False):
                    rotate_discard_counters()
                
            ws_thread.join(timeout=5.0) 
            
            log.warning("Connessione WebSocket persa. Riconnessione tra 10 secondi...")
            time.sleep(10)
            
    except KeyboardInterrupt:
        log.info("Spegnimento producer (ricevuto KeyboardInterrupt)...")
    finally:
        log.info("Chiusura connessioni finali...")
        if cassandra_session:
            cassandra_session.shutdown()
        if cassandra_cluster:
            cassandra_cluster.shutdown()

if __name__ == "__main__":
    main()