#!/usr/bin/env python3
"""
Speed Layer Consumer - Kafka to Cassandra

Legge messaggi dal topic Kafka 'crypto-prices', filtra anomalie usando
un modello EMA (Exponential Moving Average) adattivo, e scrive dati
"puliti" su Cassandra.

L'EMA si adatta continuamente alle oscillazioni dei prezzi crypto,
rendendo il filtering molto più tollerante ai trend di mercato.
"""

import os
import json
import time
import logging
import math
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

# EMA Configuration
EMA_ALPHA = float(os.getenv('EMA_ALPHA', '0.1'))  # Smoothing factor (0.1 = slow, 0.3 = reactive)
EMA_SIGMA_MULTIPLIER = float(os.getenv('EMA_SIGMA', '4.0'))  # How many std devs for bounds

# Global state - EMA model per sensor
ema_model = {}  # {sensor_id: {'mean': float, 'variance': float, 'count': int}}
initial_model = {}  # Loaded from HDFS for initialization
model_last_refresh = 0
model_mtime = 0  # Modification time of model on HDFS
cassandra_session = None
processed_count = 0
filtered_count = 0




def load_model():
    """Carica il modello iniziale da HDFS per inizializzare l'EMA."""
    global initial_model, ema_model, model_last_refresh, model_mtime
    
    try:
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1/models/model.json?op=OPEN"
        resp = requests.get(url, allow_redirects=True, timeout=10)
        if resp.status_code == 200:
            initial_model = resp.json()
            model_last_refresh = time.time()
            model_mtime = get_model_mtime()
            
            # Initialize EMA model from HDFS model
            for sensor_id, values in initial_model.items():
                if sensor_id not in ema_model:
                    mean = values.get('mean', 0)
                    std_dev = values.get('std_dev', 0)
                    ema_model[sensor_id] = {
                        'mean': mean,
                        'variance': std_dev ** 2 if std_dev else 1000000,  # variance = std^2
                        'count': values.get('count', 100)  # Assume some history
                    }
            
            logger.info(f"EMA Model initialized: {len(ema_model)} sensors")
            for sid, vals in list(ema_model.items())[:3]:
                std = math.sqrt(vals['variance']) if vals['variance'] > 0 else 0
                logger.info(f"  {sid}: mean={vals['mean']:.2f}, std={std:.2f}")
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
        logger.info("📥 Model updated on HDFS, merging new sensors...")
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


def update_ema(sensor_id, price):
    """
    Aggiorna il modello EMA per un sensor_id con un nuovo prezzo.
    Restituisce i bounds (lower, upper) calcolati.
    """
    global ema_model
    
    if sensor_id not in ema_model:
        # Nuovo sensore: inizializza con il primo valore
        ema_model[sensor_id] = {
            'mean': price,
            'variance': 1000000,  # Alta varianza iniziale = accetta tutto
            'count': 1
        }
        return (price - 100000, price + 100000)  # Bounds molto larghi
    
    model = ema_model[sensor_id]
    old_mean = model['mean']
    old_variance = model['variance']
    count = model['count']
    
    # EMA update
    # Per i primi N messaggi, usiamo un alpha più alto per stabilizzare
    effective_alpha = EMA_ALPHA if count > 20 else 0.5
    
    # Update mean
    new_mean = effective_alpha * price + (1 - effective_alpha) * old_mean
    
    # Update variance (Welford's online algorithm adapted for EMA)
    delta = price - old_mean
    new_variance = (1 - effective_alpha) * (old_variance + effective_alpha * delta * delta)
    
    # Ensure minimum variance to avoid division by zero
    new_variance = max(new_variance, 0.0001)
    
    # Update model
    ema_model[sensor_id] = {
        'mean': new_mean,
        'variance': new_variance,
        'count': count + 1
    }
    
    # Calculate bounds
    std_dev = math.sqrt(new_variance)
    lower = new_mean - EMA_SIGMA_MULTIPLIER * std_dev
    upper = new_mean + EMA_SIGMA_MULTIPLIER * std_dev
    
    return (lower, upper)


def is_valid(sensor_id, price):
    """
    Verifica se il prezzo è valido usando l'EMA adattivo.
    Aggiorna anche il modello EMA con il nuovo dato.
    """
    lower, upper = update_ema(sensor_id, price)
    
    is_ok = lower <= price <= upper
    
    if not is_ok:
        model = ema_model.get(sensor_id, {})
        std_dev = math.sqrt(model.get('variance', 0))
        logger.debug(f"Filtered: {sensor_id}={price}, bounds=[{lower:.2f}, {upper:.2f}], "
                     f"mean={model.get('mean', 0):.2f}, std={std_dev:.2f}")
    
    return is_ok


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
    global model_last_refresh
    
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
            
            # Log status every 30 seconds
            if time.time() - last_status_log > 30:
                total = processed_count + filtered_count
                if total > 0:
                    accept_rate = (processed_count / total) * 100
                    logger.info(f"📊 Status: Accepted={processed_count}, Filtered={filtered_count}, "
                               f"Rate={accept_rate:.1f}%")
                else:
                    logger.info(f"📊 Status: Waiting for data...")
                last_status_log = time.time()
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    main()
