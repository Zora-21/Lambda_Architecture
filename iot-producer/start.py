import time
import logging
import subprocess
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from hdfs import InsecureClient  # Per la connessione a HDFS

# --- Impostazioni ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CASSANDRA_HOST = 'cassandra-seed'
HDFS_HOST = 'namenode'
HDFS_PORT = 9870
HDFS_USER = 'root'
HDFS_DIR = '/iot-data'
HDFS_MODEL_PATH = '/models/model.json'

# Comandi CQL da eseguire
CQL_CREATE_KEYSPACE = """
CREATE KEYSPACE IF NOT EXISTS iot_keyspace
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2 };
"""

CQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS iot_keyspace.sensor_data (
  sensor_id TEXT,
  timestamp TIMESTAMP,
  temp FLOAT,
  PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
"""

def initialize_cassandra():
    """
    Si connette al cluster (senza keyspace) ed esegue i comandi CQL
    per creare il keyspace e la tabella.
    """
    log.info("Avvio inizializzazione Cassandra...")
    session = None
    cluster = None
    while not session:
        try:
            # Ci connettiamo senza specificare un keyspace
            # Nota: La policy è per evitare i warning visti nei log
            cluster = Cluster(
                [CASSANDRA_HOST],
                port=9042,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
            )
            session = cluster.connect()
            log.info("Connesso al cluster Cassandra (system).")
        except Exception as e:
            log.warning(f"Attesa per il cluster Cassandra... ({e})")
            time.sleep(5)

    try:
        # 1. Crea il Keyspace
        log.info("Esecuzione: Creazione Keyspace 'iot_keyspace'")
        session.execute(CQL_CREATE_KEYSPACE)
        
        # 2. Crea la Tabella
        log.info("Esecuzione: Creazione Tabella 'sensor_data'")
        session.execute(CQL_CREATE_TABLE)
        
        log.info("Keyspace 'iot_keyspace' e tabella 'sensor_data' creati/verificati.")
    
    except Exception as e:
        log.error(f"Errore durante l'esecuzione di CQL: {e}")
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()


def initialize_hdfs():
    """
    Si connette a HDFS e crea le directory base se necessarie.
    Ritorna il client HDFS.
    """
    from hdfs import InsecureClient
    
    log.info("Avvio inizializzazione HDFS...")
    hdfs_client = None
    
    while not hdfs_client:
        try:
            client = InsecureClient(f"http://{HDFS_HOST}:{HDFS_PORT}", user=HDFS_USER)
            
            # Crea directory base se non esiste
            if not client.status(HDFS_DIR, strict=False):
                log.info(f"Creazione directory HDFS di base: {HDFS_DIR}")
                client.makedirs(HDFS_DIR)
            
            # Crea directory modelli se non esiste
            if not client.status('/models', strict=False):
                log.info("Creazione directory HDFS per i modelli: /models")
                client.makedirs('/models')
            
            log.info("Connesso a HDFS!")
            hdfs_client = client
            
        except Exception as e:
            log.warning(f"Attesa per HDFS... ({e})")
            time.sleep(5)
    
    return hdfs_client


def remove_old_model(hdfs_client):
    """
    Rimuove il vecchio modello da HDFS all'avvio, per garantire una partenza pulita.
    """
    try:
        if hdfs_client.status(HDFS_MODEL_PATH, strict=False):
            log.info(f"Rimozione modello vecchio: {HDFS_MODEL_PATH}")
            hdfs_client.delete(HDFS_MODEL_PATH)
            log.info("Modello vecchio rimosso con successo.")
        else:
            log.info("Nessun modello vecchio trovato. Avvio pulito.")
    except Exception as e:
        log.error(f"Errore durante la rimozione del modello vecchio: {e}")


def main():
    """
    Script di avvio:
    1. Inizializza Cassandra (crea keyspace/tabella)
    2. Inizializza HDFS e rimuove il modello vecchio
    """
    
    # Diamo un po' di tempo ai servizi dipendenti per avviarsi
    log.info("Attesa di 15s per l'avvio dei servizi (Hadoop/Cassandra)...")
    time.sleep(15)

    initialize_cassandra()
    
    hdfs_client = initialize_hdfs()
    
    # Rimuove il modello vecchio per garantire una partenza pulita
    remove_old_model(hdfs_client)
    
    log.info("--- Inizializzazione completata ---")

if __name__ == "__main__":
    main()