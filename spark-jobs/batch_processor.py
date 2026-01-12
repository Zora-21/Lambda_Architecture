#!/usr/bin/env python3
"""
Spark Batch Processor - HDFS Data Processing

Legge dati da HDFS /iot-data/incoming/*.jsonl, filtra anomalie,
calcola metriche OHLC e indicatori tecnici, salva output.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, first, last, min, max, avg, stddev, count,
    lag, when, lit, sum as spark_sum, unix_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import requests
import time
from datetime import datetime

# Configuration
HDFS_NAMENODE = "hdfs://namenode:9000"
INPUT_PATH = f"{HDFS_NAMENODE}/iot-data/incoming/*.jsonl"
INPUT_DIR = f"{HDFS_NAMENODE}/iot-data/incoming"
OUTPUT_PATH = f"{HDFS_NAMENODE}/iot-output/spark"
ARCHIVE_PATH = f"{HDFS_NAMENODE}/iot-data/archive"
MODEL_PATH = f"{HDFS_NAMENODE}/models/model.json"
AGGREGATE_STATS_PATH = f"{HDFS_NAMENODE}/iot-stats/daily-aggregate"


def load_model(spark):
    """Load anomaly detection model from HDFS."""
    try:
        model_rdd = spark.sparkContext.textFile(MODEL_PATH)
        model_json = model_rdd.collect()
        if model_json:
            return json.loads(model_json[0])
    except Exception as e:
        print(f"Could not load model: {e}")
    return {}


def get_model_age(spark):
    """
    Returns the age of the model file in seconds.
    Returns -1 if model does not exist or error.
    """
    try:
        sc = spark.sparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE),
            sc._jsc.hadoopConfiguration()
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(MODEL_PATH)
        
        if fs.exists(path):
            status = fs.getFileStatus(path)
            mod_time = status.getModificationTime() # ms
            current_time = time.time() * 1000 # ms
            age_sec = (current_time - mod_time) / 1000.0
            return age_sec
    except Exception as e:
        print(f"Error checking model age: {e}")
        
    return -1


def clear_incoming_files(spark):
    """
    Elimina tutti i file .jsonl dalla cartella /incoming dopo l'archiviazione.
    Questo previene il riprocessamento degli stessi dati.
    """
    try:
        sc = spark.sparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE),
            sc._jsc.hadoopConfiguration()
        )
        incoming_path = spark._jvm.org.apache.hadoop.fs.Path("/iot-data/incoming")
        
        if fs.exists(incoming_path):
            # Lista tutti i file nella directory
            file_status_list = fs.listStatus(incoming_path)
            deleted_count = 0
            
            for file_status in file_status_list:
                file_path = file_status.getPath()
                file_name = file_path.getName()
                
                # Elimina solo i file .jsonl (non toccare altre estensioni)
                if file_name.endswith('.jsonl'):
                    fs.delete(file_path, False)  # False = non ricorsivo
                    deleted_count += 1
            
            print(f"✅ Cleaned up {deleted_count} processed files from /incoming")
            return deleted_count
    except Exception as e:
        print(f"⚠️ Warning: Could not clear incoming files: {e}")
    
    return 0


def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("LambdaBatchProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("Spark Batch Processor Starting...")
    print("=" * 60)
    
    # Define schema
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("source", StringType(), True)
    ])
    
    # Load data
    try:
        df = spark.read.schema(schema).json(INPUT_PATH)
        record_count = df.count()
        print(f"Loaded {record_count} records from HDFS")
        
        if record_count == 0:
            print("No data to process")
            spark.stop()
            return
    except Exception as e:
        print(f"Error loading data: {e}")
        spark.stop()
        return
    
    # Load model for filtering
    model = load_model(spark)
    print(f"Loaded model with {len(model)} sensors")
    
    # Filter anomalies if model exists
    # Track counts for aggregate stats (like old project's reducer)
    total_count = record_count
    clean_count = record_count
    discarded_count = 0
    
    if model:
        model_broadcast = spark.sparkContext.broadcast(model)
        
        def is_valid(sensor_id, price):
            m = model_broadcast.value
            if sensor_id not in m:
                return True
            mean = m[sensor_id].get('mean', price)
            std_dev = m[sensor_id].get('std_dev', 1000)
            if std_dev == 0:
                return True
            return (mean - 3 * std_dev) <= price <= (mean + 3 * std_dev)
        
        # Register UDF
        from pyspark.sql.functions import udf
        from pyspark.sql.types import BooleanType
        is_valid_udf = udf(is_valid, BooleanType())
        
        df_filtered = df.filter(is_valid_udf(col("sensor_id"), col("temp")))
        clean_count = df_filtered.count()
        discarded_count = total_count - clean_count
        print(f"After filtering: {clean_count} records (removed {discarded_count} anomalies)")
    else:
        df_filtered = df
        print("No model available, skipping anomaly filtering")
    
    # Add numeric timestamp for ordering
    df_with_ts = df_filtered.withColumn(
        "ts_numeric", 
        unix_timestamp(col("timestamp"))
    )
    
    # Define windows for technical indicators
    window_spec = Window.partitionBy("sensor_id").orderBy("ts_numeric")
    window_rsi = Window.partitionBy("sensor_id").orderBy("ts_numeric").rowsBetween(-14, 0)
    window_bb = Window.partitionBy("sensor_id").orderBy("ts_numeric").rowsBetween(-20, 0)
    
    # Calculate price changes
    df_with_change = df_with_ts.withColumn(
        "price_change", col("temp") - lag("temp", 1).over(window_spec)
    ).withColumn(
        "gain", when(col("price_change") > 0, col("price_change")).otherwise(0)
    ).withColumn(
        "loss", when(col("price_change") < 0, -col("price_change")).otherwise(0)
    ).withColumn(
        "price_10_ago", lag("temp", 10).over(window_spec)
    )
    
    # Calculate RSI components
    df_with_rsi = df_with_change.withColumn(
        "avg_gain", avg("gain").over(window_rsi)
    ).withColumn(
        "avg_loss", avg("loss").over(window_rsi)
    ).withColumn(
        "rs", when(col("avg_loss") > 0, col("avg_gain") / col("avg_loss")).otherwise(100)
    ).withColumn(
        "rsi", 100 - (100 / (1 + col("rs")))
    )
    
    # Calculate Bollinger Bands
    df_with_bb = df_with_rsi.withColumn(
        "sma_20", avg("temp").over(window_bb)
    ).withColumn(
        "std_20", stddev("temp").over(window_bb)
    ).withColumn(
        "bollinger_upper", col("sma_20") + 2 * col("std_20")
    ).withColumn(
        "bollinger_lower", col("sma_20") - 2 * col("std_20")
    )
    
    # Calculate Momentum
    df_with_momentum = df_with_bb.withColumn(
        "momentum", 
        when(col("price_10_ago") > 0, 
             ((col("temp") - col("price_10_ago")) / col("price_10_ago")) * 100
        ).otherwise(0)
    )
    
    # OHLC + Technical Indicators Aggregation
    result = df_with_momentum.groupBy("sensor_id").agg(
        first("temp").alias("open"),
        last("temp").alias("close"),
        min("temp").alias("min"),
        max("temp").alias("max"),
        avg("temp").alias("mean"),
        stddev("temp").alias("volatility"),
        count("*").alias("count"),
        avg("rsi").alias("avg_rsi"),
        avg("bollinger_upper").alias("bollinger_upper"),
        avg("bollinger_lower").alias("bollinger_lower"),
        avg("momentum").alias("avg_momentum")
    )
    
    # Calculate daily change
    result = result.withColumn(
        "daily_change", col("close") - col("open")
    ).withColumn(
        "daily_change_pct", 
        when(col("open") > 0, (col("daily_change") / col("open")) * 100).otherwise(0)
    ).withColumn(
        "processed_at", lit(datetime.utcnow().isoformat() + "Z")
    )
    
    # Show results
    print("\n" + "=" * 60)
    print("BATCH PROCESSING RESULTS")
    print("=" * 60)
    result.show(truncate=False)
    
    # Save results with INCREMENTAL count
    today = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{OUTPUT_PATH}/date={today}"
    
    # --- IMPORTANTE: Check PRIMA se è una calibration run ---
    # Calibration = PRIMO run del giorno dove OHLC output non esiste ancora
    # E il modello è stato creato di recente (< 120s)
    
    is_calibration_run = False
    try:
        sc = spark.sparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE),
            sc._jsc.hadoopConfiguration()
        )
        ohlc_output_dir = spark._jvm.org.apache.hadoop.fs.Path(f"/iot-output/spark/date={today}")
        
        # Se l'output OHLC NON esiste, è la prima run del giorno
        if not fs.exists(ohlc_output_dir):
            model_age = get_model_age(spark)
            # Solo se il modello è stato appena creato (< 120s), è calibrazione
            if 0 <= model_age < 120:
                is_calibration_run = True
                print(f"📋 First run of the day + fresh model ({model_age:.1f}s) = Calibration")
            else:
                print(f"📋 First run of the day but model is old ({model_age:.1f}s) - NOT calibration")
        else:
            print(f"📋 OHLC output exists for today - NOT calibration")
    except Exception as e:
        print(f"Error checking calibration status: {e}")
    
    # --- Rendi il count incrementale (SOLO se NON è calibration run) ---
    existing_counts = {}
    read_existing_success = False
    
    if not is_calibration_run:
        try:
            sc = spark.sparkContext
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(HDFS_NAMENODE),
                sc._jsc.hadoopConfiguration()
            )
            output_dir = spark._jvm.org.apache.hadoop.fs.Path(f"/iot-output/spark/date={today}")
            
            if fs.exists(output_dir):
                file_status_list = fs.listStatus(output_dir)
                for file_status in file_status_list:
                    file_path = file_status.getPath()
                    file_name = file_path.getName()
                    
                    if file_name.startswith('part-') and file_name.endswith('.json'):
                        input_stream = fs.open(file_path)
                        reader = spark._jvm.java.io.BufferedReader(
                            spark._jvm.java.io.InputStreamReader(input_stream, "UTF-8")
                        )
                        line = reader.readLine()
                        while line is not None:
                            if line.strip():
                                existing_data = json.loads(line)
                                sensor_id = existing_data.get("sensor_id")
                                if sensor_id:
                                    existing_counts[sensor_id] = existing_data.get("count", 0)
                            line = reader.readLine()
                        reader.close()
                        input_stream.close()
                
                if existing_counts:
                    print(f"✅ Found existing OHLC counts: {existing_counts}")
                    read_existing_success = True
                else:
                    print("⚠️ OHLC output exists but no counts found - will NOT reset!")
            else:
                print("📝 No existing OHLC output - starting fresh for today")
                read_existing_success = True  # OK, è il primo run
        except Exception as e:
            print(f"❌ ERROR reading existing counts: {e}")
            print("   -> CRITICAL: Will NOT save to prevent data loss!")
            # Se non possiamo leggere i vecchi dati, NON sovrascriviamo!
            # Questo previene la perdita dei contatori
        
        # Aggiungi i conteggi esistenti ai nuovi
        if existing_counts:
            # Converti in lista, aggiorna i count, e ricrea il DataFrame
            result_list = [row.asDict() for row in result.collect()]
            for row in result_list:
                sensor_id = row.get("sensor_id")
                if sensor_id in existing_counts:
                    row["count"] = row["count"] + existing_counts[sensor_id]
            
            # Ricrea il DataFrame con i count aggiornati
            result = spark.createDataFrame(result_list)
            print("✅ Updated counts with existing values (INCREMENTAL)")
    else:
        print("⏭️ Calibration run detected - OHLC counts will NOT be incremented")
        read_existing_success = True  # OK per calibration

    
    # --- SAVE OHLC (solo se sicuro) ---
    if read_existing_success:
        try:
            result.coalesce(1).write.mode("overwrite").json(output_path)
            print(f"Results saved to: {output_path}")
        except Exception as e:
            print(f"Error saving results: {e}")
    else:
        print("❌ SKIPPING OHLC SAVE: Could not read existing counts safely!")
        print("   -> This prevents counter reset. Fix the underlying issue.")
    
    # Archive processed data
    archive_path = f"{ARCHIVE_PATH}/date={today}"
    archive_success = False
    try:
        df_filtered.coalesce(1).write.mode("append").json(archive_path)
        print(f"Data archived to: {archive_path}")
        archive_success = True
    except Exception as e:
        print(f"Error archiving data: {e}")
    
    # --- IMPORTANTE: Elimina i file processati da /incoming ---
    # Questo previene il doppio conteggio alla prossima esecuzione
    if archive_success:
        clear_incoming_files(spark)
    else:
        print("⚠️ Skipping cleanup: archive failed, data preserved for retry")
    
    # --- Write aggregate stats (like old project's aggregate_stats.py) ---
    # (is_calibration_run già calcolato sopra)
    
    if is_calibration_run:
        print("\n" + "!" * 60)
        print(f"SKIPPING STATS: Model is brand new ({model_age:.1f}s old).")
        print("This is likely a Calibration Run. Data archived but not counted in daily stats.")
        print("!" * 60 + "\n")
    else:
        # --- CORREZIONE v2: Usa un file JSON singolo invece di directory Spark ---
        # Questo evita il problema dove mode("overwrite") elimina la directory prima della lettura
        stats_file_path = f"/iot-stats/daily-aggregate/stats_{today}.json"
        
        # Inizializza con zero
        existing_clean = 0
        existing_discarded = 0
        existing_processed = 0
        
        try:
            # Leggi il file esistente usando l'API HDFS FileSystem
            sc = spark.sparkContext
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(HDFS_NAMENODE),
                sc._jsc.hadoopConfiguration()
            )
            file_path = spark._jvm.org.apache.hadoop.fs.Path(stats_file_path)
            
            if fs.exists(file_path):
                # Leggi il contenuto del file
                input_stream = fs.open(file_path)
                reader = spark._jvm.java.io.BufferedReader(
                    spark._jvm.java.io.InputStreamReader(input_stream, "UTF-8")
                )
                content = ""
                line = reader.readLine()
                while line is not None:
                    content += line
                    line = reader.readLine()
                reader.close()
                input_stream.close()
                
                if content:
                    existing_data = json.loads(content)
                    existing_clean = existing_data.get("total_clean", 0)
                    existing_discarded = existing_data.get("total_discarded", 0)
                    existing_processed = existing_data.get("total_processed", 0)
                    print(f"Found existing stats: Clean={existing_clean}, Discarded={existing_discarded}, Total={existing_processed}")
        except Exception as e:
            # File non esiste ancora, parti da zero
            print(f"No existing stats found (starting fresh): {e}")
        
        # Somma i nuovi valori a quelli esistenti (INCREMENTALE)
        # IMPORTANTE: total_clean = sum dei counts OHLC (che sono GIÀ incrementali dalla linea 331)
        # Quindi NON aggiungiamo existing_clean, altrimenti conteremmo due volte!
        ohlc_total_clean = 0
        try:
            result_data = [row.asDict() for row in result.collect()]
            ohlc_total_clean = sum(row.get("count", 0) for row in result_data)
            print(f"OHLC sum of all sensor counts: {ohlc_total_clean}")
        except Exception as e:
            print(f"Warning: could not calculate OHLC sum: {e}")
            # Fallback: usa existing + new clean count
            ohlc_total_clean = existing_clean + clean_count
        
        aggregate_stats = {
            "total_clean": ohlc_total_clean,  # Somma dei counts OHLC (già incrementali)
            "total_discarded": existing_discarded + discarded_count,
            "total_processed": existing_processed + total_count
        }
        
        # Solo salvare se la lettura OHLC è andata bene
        if read_existing_success:
            try:
                # Scrivi il file JSON tramite HDFS FileSystem API
                sc = spark.sparkContext
                fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark._jvm.java.net.URI(HDFS_NAMENODE),
                    sc._jsc.hadoopConfiguration()
                )
                
                # Assicurati che la directory esista
                dir_path = spark._jvm.org.apache.hadoop.fs.Path("/iot-stats/daily-aggregate")
                if not fs.exists(dir_path):
                    fs.mkdirs(dir_path)
                
                file_path = spark._jvm.org.apache.hadoop.fs.Path(stats_file_path)
                
                # Scrivi il nuovo contenuto (sovrascrive il file precedente)
                output_stream = fs.create(file_path, True)  # True = overwrite
                output_stream.write(json.dumps(aggregate_stats).encode('utf-8'))
                output_stream.close()
                
                print(f"Aggregate stats saved to: {stats_file_path}")
                print(f"  -> This batch: Total={total_count}, Clean={clean_count}, Discarded={discarded_count}")
                print(f"  -> Cumulative: Total={aggregate_stats['total_processed']}, Clean={aggregate_stats['total_clean']}, Discarded={aggregate_stats['total_discarded']}")
            except Exception as e:
                print(f"Error saving aggregate stats: {e}")
        else:
            print("❌ SKIPPING AGGREGATE STATS SAVE: OHLC read failed, stats would be inconsistent!")
    
    print("\n" + "=" * 60)
    print("Batch Processing Complete!")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
