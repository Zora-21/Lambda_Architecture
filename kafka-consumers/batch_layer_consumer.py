#!/usr/bin/env python3
"""
Batch Layer Consumer - Kafka to HDFS

Legge messaggi dal topic Kafka 'crypto-prices', accumula in buffer,
e scrive batch periodici su HDFS senza filtraggio.
"""

import os
import json
import time
import logging
import requests
from kafka import KafkaConsumer
from datetime import datetime

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto-prices')
KAFKA_GROUP_ID = os.getenv('KAFKA_BATCH_GROUP_ID', 'batch-layer-group')
HDFS_HOST = os.getenv('HDFS_HOST', 'namenode')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '5000'))
BATCH_FLUSH_INTERVAL = int(os.getenv('BATCH_FLUSH_INTERVAL', '180'))
LOCAL_BUFFER_PATH = '/buffer/hdfs_buffer.jsonl'

# Global state
message_buffer = []
last_flush_time = time.time()
total_written = 0
is_in_calibration = True  # Inizia in calibrazione, diventa False quando il modello esiste


def write_to_hdfs(data_lines):
    """Scrive un batch di dati su HDFS via WebHDFS."""
    global total_written
    
    if not data_lines:
        return True
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    filename = f"batch_{timestamp}.jsonl"
    hdfs_path = f"/iot-data/incoming/{filename}"
    
    # Prepare data
    content = '\n'.join([json.dumps(line) for line in data_lines])
    
    try:
        # Step 1: Create file request (with user.name for permission)
        create_url = f"http://{HDFS_HOST}:9870/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
        resp = requests.put(create_url, allow_redirects=False, timeout=30)
        
        if resp.status_code == 307:
            # Step 2: Follow redirect and write data
            redirect_url = resp.headers['Location']
            write_resp = requests.put(
                redirect_url, 
                data=content.encode('utf-8'),
                headers={'Content-Type': 'application/octet-stream'},
                timeout=60
            )
            
            if write_resp.status_code in [200, 201]:
                total_written += len(data_lines)
                logger.info(f"Wrote {len(data_lines)} records to HDFS: {hdfs_path}")
                return True
            else:
                logger.error(f"HDFS write failed: HTTP {write_resp.status_code}")
        else:
            logger.error(f"HDFS create failed: HTTP {resp.status_code}")
            
    except Exception as e:
        logger.error(f"HDFS write error: {e}")
        # Save to local buffer for recovery
        save_local_buffer(data_lines)
    
    return False


def save_local_buffer(data_lines):
    """Salva i dati in un buffer locale per recovery."""
    try:
        os.makedirs(os.path.dirname(LOCAL_BUFFER_PATH), exist_ok=True)
        with open(LOCAL_BUFFER_PATH, 'a') as f:
            for line in data_lines:
                f.write(json.dumps(line) + '\n')
        logger.info(f"Saved {len(data_lines)} records to local buffer")
    except Exception as e:
        logger.error(f"Failed to save local buffer: {e}")


def load_local_buffer():
    """Carica e svuota il buffer locale."""
    records = []
    try:
        if os.path.exists(LOCAL_BUFFER_PATH):
            with open(LOCAL_BUFFER_PATH, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
            if records:
                logger.info(f"Loaded {len(records)} records from local buffer")
                # Clear buffer after loading
                os.remove(LOCAL_BUFFER_PATH)
    except Exception as e:
        logger.error(f"Failed to load local buffer: {e}")
    return records


def flush_buffer():
    """Svuota il buffer scrivendo su HDFS."""
    global message_buffer, last_flush_time
    
    if not message_buffer:
        last_flush_time = time.time()
        return
    
    # Copy and clear buffer
    to_write = message_buffer.copy()
    message_buffer = []
    
    if write_to_hdfs(to_write):
        last_flush_time = time.time()
    else:
        # Restore buffer on failure
        message_buffer = to_write + message_buffer


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


def main():
    global message_buffer, last_flush_time, is_in_calibration
    
    logger.info("Batch Layer Consumer starting...")
    logger.info(f"Config: BATCH_SIZE={BATCH_SIZE}, FLUSH_INTERVAL={BATCH_FLUSH_INTERVAL}s")
    
    # NO MODEL WAIT - batch layer writes to HDFS immediately
    # This allows model_trainer to use calibration data from HDFS
    
    # Create Kafka consumer - use 'earliest' to capture all data including calibration
    logger.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    
    max_retries = 30
    consumer = None
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=500,
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
    
    logger.info("🚀 Starting to process messages (writing to HDFS)...")
    
    # Main processing loop
    last_flush_time = time.time()
    last_model_check = 0
    MODEL_CHECK_INTERVAL = 30  # Controlla ogni 30 secondi se il modello esiste
    
    for message in consumer:
        try:
            data = message.value
            
            # Controlla periodicamente se il modello esiste (per uscire dalla calibrazione)
            if is_in_calibration and (time.time() - last_model_check) >= MODEL_CHECK_INTERVAL:
                last_model_check = time.time()
                if check_model_exists():
                    is_in_calibration = False
                    logger.info("✅ Model detected! Exiting calibration mode - future data will be used for OHLC")
            
            # Marca i dati come calibrazione se siamo ancora in fase di calibrazione
            if is_in_calibration:
                data['is_calibration'] = True
            
            message_buffer.append(data)
            
            # Check flush conditions
            buffer_full = len(message_buffer) >= BATCH_SIZE
            time_exceeded = (time.time() - last_flush_time) >= BATCH_FLUSH_INTERVAL
            
            if buffer_full or time_exceeded:
                reason = "size" if buffer_full else "time"
                cal_info = " [CALIBRATION]" if is_in_calibration else ""
                logger.info(f"Flushing buffer ({reason}): {len(message_buffer)} records{cal_info}")
                flush_buffer()
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    main()
