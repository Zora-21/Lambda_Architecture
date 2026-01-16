#!/usr/bin/env python3
"""
Spark Scheduler - Simplified Version

Workflow:
1. Wait 5 minutes for batch-layer-consumer to write calibration data to HDFS
2. Run model_trainer.py on HDFS data
3. Model created, speed-layer-consumer unblocks
4. Start normal periodic jobs
"""

import os
import time
import logging
import subprocess
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SCHEDULER - %(message)s'
)
log = logging.getLogger(__name__)

# Configuration
SPARK_MASTER = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
HDFS_HOST = os.getenv('HDFS_HOST', 'namenode')
JOBS_DIR = '/opt/spark/jobs'
MODEL_PATH = '/models/model.json'
CALIBRATION_WAIT = 300  # 5 minuti
# Event-driven mode: batch_processor runs when new data arrives (MIN_BATCH_INTERVAL = 30s)

last_batch_run = 0


def check_model_exists():
    """Verifica se il modello esiste su HDFS."""
    try:
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1{MODEL_PATH}?op=GETFILESTATUS"
        resp = requests.get(url, timeout=10)
        return resp.status_code == 200
    except:
        return False


def run_spark_job(job_name):
    """Esegue un job Spark."""
    job_path = f"{JOBS_DIR}/{job_name}"
    
    log.info(f"🚀 Starting Spark job: {job_name}")
    start_time = time.time()
    
    try:
        result = subprocess.run(
            [
                '/opt/spark/bin/spark-submit',
                '--master', SPARK_MASTER,
                '--deploy-mode', 'client',
                '--driver-memory', '512m',
                '--executor-memory', '512m',
                job_path
            ],
            capture_output=True,
            text=True,
            timeout=600
        )
        
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            log.info(f"✅ Job {job_name} completed in {elapsed:.1f}s")
            # Log key output lines
            for line in result.stdout.strip().split('\n')[-10:]:
                if line.strip() and ('Model' in line or 'saved' in line or 'sensor' in line.lower()):
                    log.info(f"   {line}")
            return True
        else:
            log.error(f"❌ Job {job_name} failed")
            for line in result.stderr.strip().split('\n')[-5:]:
                if line.strip():
                    log.error(f"   {line}")
            return False
            
    except subprocess.TimeoutExpired:
        log.error(f"❌ Job {job_name} timed out")
        return False
    except Exception as e:
        log.error(f"❌ Failed to run {job_name}: {e}")
        return False


def check_hdfs_data():
    """Verifica se ci sono dati da processare su HDFS."""
    try:
        url = f"http://{HDFS_HOST}:9870/webhdfs/v1/iot-data/incoming?op=LISTSTATUS"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            files = resp.json().get('FileStatuses', {}).get('FileStatus', [])
            return len(files) > 0
    except:
        pass
    return False


def main():
    global last_batch_run
    
    log.info("=" * 60)
    log.info("Spark Scheduler Starting...")
    log.info(f"  Calibration Wait: {CALIBRATION_WAIT}s")
    log.info("  Mode: EVENT-DRIVEN (triggers on new data)")
    log.info("  Min Batch Interval: 30s")
    log.info("=" * 60)
    
    # Wait for Spark to be ready
    log.info("Waiting for Spark Master to be ready...")
    time.sleep(30)
    
    # CALIBRATION PHASE
    if not check_model_exists():
        log.info("=" * 60)
        log.info("🔄 CALIBRATION PHASE")
        log.info("=" * 60)
        log.info(f"Waiting {CALIBRATION_WAIT}s for data collection on HDFS...")
        
        # Countdown
        for remaining in range(CALIBRATION_WAIT, 0, -30):
            minutes = remaining // 60
            seconds = remaining % 60
            log.info(f"  ⏳ Time remaining: {minutes}m {seconds}s")
            time.sleep(30)
        
        log.info("⏰ Calibration wait complete, running model trainer...")
        
        # Run model trainer
        if run_spark_job('model_trainer.py'):
            if check_model_exists():
                log.info("=" * 60)
                log.info("✅ CALIBRATION COMPLETE - Model created")
                log.info("   (Model will be updated incrementally by batch_processor)")
                log.info("=" * 60)
            else:
                log.error("❌ Model trainer ran but model not found - retrying...")
                time.sleep(60)
                run_spark_job('model_trainer.py')
        else:
            log.error("❌ Model training failed - retrying in 60s...")
            time.sleep(60)
            run_spark_job('model_trainer.py')
    else:
        log.info("✅ Model already exists, skipping calibration")
    
    log.info("🚀 Starting normal operations (EVENT-DRIVEN mode)")
    log.info("   Batch processor will run when new data arrives in HDFS")
    last_batch_run = 0
    MIN_BATCH_INTERVAL = 30  # Minimo 30s tra esecuzioni per evitare overhead
    
    while True:
        now = time.time()
        
        # Event-driven: controlla se ci sono dati e processa subito
        if check_hdfs_data():
            # Rispetta intervallo minimo per evitare overhead Spark
            if now - last_batch_run >= MIN_BATCH_INTERVAL:
                log.info("📥 New data detected in HDFS, triggering batch processor...")
                if run_spark_job('batch_processor.py'):
                    last_batch_run = now
                else:
                    log.warning("Batch processor failed, will retry on next data check")
            else:
                remaining = int(MIN_BATCH_INTERVAL - (now - last_batch_run))
                log.debug(f"Data available but waiting {remaining}s before next run")
        
        # Controlla ogni 5 secondi per nuovi dati
        time.sleep(5)


if __name__ == "__main__":
    main()
