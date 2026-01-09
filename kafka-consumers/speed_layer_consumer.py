#!/usr/bin/env python3
"""
Speed Layer Consumer - Kafka to Cassandra

Legge messaggi dal topic Kafka 'crypto-prices', filtra anomalie usando
il modello 3-sigma, e scrive dati "puliti" su Cassandra.
"""

import os
import json
import time
import logging
import requests
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto-prices')
KAFKA_GROUP_ID = os.getenv('KAFKA_SPEED_GROUP_ID', 'speed-layer-group')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'cassandra-seed')
HDFS_HOST = os.getenv('HDFS_HOST', 'namenode')
MODEL_REFRESH_INTERVAL = 300  # 5 minutes

# Global state
filtering_model = {}
model_last_refresh = 0
model_mtime = 0  # Modification time of model on HDFS
cassandra_session = None
processed_count = 0
filtered_count = 0
HDFS_DISCARD_STATS_PATH = '/models/discard_stats.json'


def flush_discard_stats(count):
    """Scrive il conteggio anomalie su HDFS."""
    try:
        # Write via WebHDFS
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1{HDFS_DISCARD_STATS_PATH}?op=CREATE&overwrite=true&user.name=root"
        resp = requests.put(url, allow_redirects=False, timeout=10)
        if resp.status_code == 307:
            redirect_url = resp.headers['Location']
            data = json.dumps({"total": count})
            requests.put(redirect_url, data=data.encode('utf-8'), timeout=10)
            logger.debug(f"Discard stats flushed: {count}")
    except Exception as e:
        logger.warning(f"Failed to flush discard stats: {e}")


def load_model():
    """Carica il modello di filtering da HDFS."""
    global filtering_model, model_last_refresh, model_mtime
    
    try:
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1/models/model.json?op=OPEN"
        resp = requests.get(url, allow_redirects=True, timeout=10)
        if resp.status_code == 200:
            filtering_model = resp.json()
            model_last_refresh = time.time()
            # Get modification time
            model_mtime = get_model_mtime()
            logger.info(f"Model loaded successfully: {len(filtering_model)} sensors")
            return True
        else:
            logger.warning(f"Failed to load model: HTTP {resp.status_code}")
            return False
    except Exception as e:
        logger.warning(f"Could not load model from HDFS: {e}")
        return False


def get_model_mtime():
    """Ottiene il timestamp di modifica del modello su HDFS."""
    try:
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1/models/model.json?op=GETFILESTATUS"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return resp.json().get('FileStatus', {}).get('modificationTime', 0)
    except:
        pass
    return 0


def check_model_updated():
    """Controlla se il modello è stato aggiornato (throttled ogni 10s)."""
    global model_last_refresh
    
    # Throttle: controlla solo ogni 10 secondi
    if time.time() - model_last_refresh < 10:
        return False
    
    model_last_refresh = time.time()
    current_mtime = get_model_mtime()
    if current_mtime > model_mtime:
        logger.info("📥 Model updated on HDFS, reloading...")
        return load_model()
    return False


def check_model_exists():
    """Controlla se il modello esiste su HDFS."""
    try:
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1/models/model.json?op=GETFILESTATUS"
        resp = requests.get(url, timeout=10)
        return resp.status_code == 200
    except:
        return False


def wait_for_model():
    """Aspetta che il modello sia creato durante la calibrazione."""
    logger.info("=" * 60)
    logger.info("🔄 CALIBRATION PHASE - Waiting for model...")
    logger.info("=" * 60)
    
    while not check_model_exists():
        logger.info("  ⏳ Model not ready, waiting 30 seconds...")
        time.sleep(30)
    
    logger.info("✅ Model is ready!")
    logger.info("=" * 60)


def is_valid(sensor_id, price):
    """Verifica se il prezzo è valido usando la regola 3-sigma."""
    if sensor_id not in filtering_model:
        return True  # Accept unknown sensors
    
    model = filtering_model[sensor_id]
    mean = model.get('mean', price)
    std_dev = model.get('std_dev', 1000)
    
    lower_bound = mean - 3 * std_dev
    upper_bound = mean + 3 * std_dev
    
    return lower_bound <= price <= upper_bound


def setup_cassandra():
    """Connessione a Cassandra."""
    global cassandra_session
    
    max_retries = 30
    for attempt in range(max_retries):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            cassandra_session = cluster.connect()
            cassandra_session.set_keyspace('iot_keyspace')
            logger.info("Connected to Cassandra")
            return True
        except Exception as e:
            logger.warning(f"Cassandra connection attempt {attempt + 1}/{max_retries}: {e}")
            time.sleep(5)
    
    logger.error("Failed to connect to Cassandra")
    return False


def write_to_cassandra(data):
    """Scrive un record su Cassandra."""
    global processed_count
    
    try:
        query = """
            INSERT INTO sensor_data (sensor_id, timestamp, temp)
            VALUES (%s, %s, %s)
        """
        cassandra_session.execute(query, (
            data['sensor_id'],
            data['timestamp'],
            data['temp']
        ))
        processed_count += 1
        return True
    except Exception as e:
        logger.error(f"Cassandra write error: {e}")
        return False


def main():
    global filtered_count, model_last_refresh
    
    logger.info("Speed Layer Consumer starting...")
    
    # Setup connections
    if not setup_cassandra():
        logger.error("Cannot start without Cassandra connection")
        return
    
    # WAIT FOR MODEL (CALIBRATION GATE)
    wait_for_model()
    
    # Load the model
    load_model()
    
    # Create Kafka consumer - use 'latest' to skip calibration data
    logger.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    logger.info("Using 'latest' offset to skip calibration data")
    
    max_retries = 30
    consumer = None
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Connected to Kafka")
            break
        except Exception as e:
            logger.warning(f"Kafka connection attempt {attempt + 1}/{max_retries}: {e}")
            time.sleep(5)
    
    if not consumer:
        logger.error("Cannot start without Kafka connection")
        return
    
    logger.info("🚀 Starting to process messages...")
    
    # Main processing loop
    last_status_log = time.time()
    
    for message in consumer:
        try:
            data = message.value
            sensor_id = data.get('sensor_id')
            price = data.get('temp', data.get('price'))
            
            # Check if model was updated (event-based, not time-based)
            check_model_updated()
            
            # Filter anomalies
            if is_valid(sensor_id, price):
                # Normalize data format
                record = {
                    'sensor_id': sensor_id,
                    'timestamp': data.get('timestamp'),
                    'temp': price
                }
                write_to_cassandra(record)
            else:
                filtered_count += 1
                logger.debug(f"Filtered anomaly: {sensor_id}={price}")
            
            # Log status every 30 seconds and flush discard stats
            if time.time() - last_status_log > 30:
                logger.info(f"Status: Processed={processed_count}, Filtered={filtered_count}")
                last_status_log = time.time()
                # Flush discard stats to HDFS
                flush_discard_stats(filtered_count)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    main()
