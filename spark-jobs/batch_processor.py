#!/usr/bin/env python3
"""
Spark Batch Processor - HDFS Data Processing (Stateful Chunk-Based Filtering)

Legge dati da HDFS, applica un filtro adattivo basato su statistiche globali
(3-Sigma con modello persistente), processa in chunk da 100 record,
calcola metriche OHLC/RSI/Bollinger e aggiorna le statistiche incrementalmente.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, first, last, min, max, avg, stddev, count,
    lag, when, lit, sum as spark_sum, unix_timestamp, abs as spark_abs, row_number, floor
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import time
import math
from datetime import datetime

# --- Configurazione ---
HDFS_NAMENODE = "hdfs://namenode:9000"
INPUT_PATH = f"{HDFS_NAMENODE}/iot-data/incoming/*.jsonl"
OUTPUT_PATH = f"{HDFS_NAMENODE}/iot-output/spark"
ARCHIVE_PATH = f"{HDFS_NAMENODE}/iot-data/archive"
MODEL_PATH = f"{HDFS_NAMENODE}/models/model.json"
STATS_FILE_PATH = "/iot-stats/daily-aggregate"

# Parametri di filtraggio
CHUNK_SIZE = 150  # Record per chunk
SIGMA_THRESHOLD = 4.0  # Soglia 3-sigma


def load_model(spark):
    """
    Carica il modello da HDFS.
    Ritorna un dizionario: {sensor_id: {"mean": float, "variance": float, "count": int}}
    """
    model = {}
    try:
        sc = spark.sparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE),
            sc._jsc.hadoopConfiguration()
        )
        path = spark._jvm.org.apache.hadoop.fs.Path(MODEL_PATH)
        
        if fs.exists(path):
            stream = fs.open(path)
            reader = spark._jvm.java.io.BufferedReader(
                spark._jvm.java.io.InputStreamReader(stream)
            )
            content = reader.readLine()
            reader.close()
            stream.close()
            
            if content:
                raw_model = json.loads(content)
                # Normalizza il modello (converti std_dev in variance se necessario)
                for sensor_id, stats in raw_model.items():
                    mean = stats.get("mean", 0)
                    # Supporta sia variance che std_dev
                    if "variance" in stats:
                        variance = stats["variance"]
                    elif "std_dev" in stats:
                        variance = stats["std_dev"] ** 2
                    else:
                        variance = 0
                    count_val = stats.get("count", 0)
                    
                    model[sensor_id] = {
                        "mean": mean,
                        "variance": variance,
                        "count": count_val
                    }
                print(f"✅ Model loaded: {len(model)} sensors")
                for sid, s in model.items():
                    print(f"   {sid}: mean={s['mean']:.2f}, std={math.sqrt(s['variance']):.4f}, n={s['count']}")
    except Exception as e:
        print(f"⚠️ Could not load model: {e}")
    
    return model


def save_model(spark, model):
    """
    Salva il modello aggiornato su HDFS.
    """
    try:
        sc = spark.sparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE),
            sc._jsc.hadoopConfiguration()
        )
        
        # Converti variance in std_dev per compatibilità
        save_model = {}
        for sensor_id, stats in model.items():
            save_model[sensor_id] = {
                "mean": round(stats["mean"], 6),
                "std_dev": round(math.sqrt(stats["variance"]), 6) if stats["variance"] > 0 else 0,
                "variance": round(stats["variance"], 6),
                "count": stats["count"]
            }
        
        model_json = json.dumps(save_model)
        
        # Crea directory se non esiste
        models_dir = spark._jvm.org.apache.hadoop.fs.Path("/models")
        if not fs.exists(models_dir):
            fs.mkdirs(models_dir)
        
        # Scrivi modello
        path = spark._jvm.org.apache.hadoop.fs.Path(MODEL_PATH)
        out = fs.create(path, True)
        out.write(model_json.encode('utf-8'))
        out.close()
        
        print(f"✅ Model saved to {MODEL_PATH}")
        return True
    except Exception as e:
        print(f"❌ Error saving model: {e}")
        return False


def update_model_incremental(model, sensor_id, new_values):
    """
    Aggiorna le statistiche del modello in modo incrementale (Welford's algorithm).
    
    Args:
        model: dizionario del modello
        sensor_id: ID del sensore
        new_values: lista di valori float (dati puliti del chunk)
    
    Returns:
        model aggiornato
    """
    if not new_values:
        return model
    
    if sensor_id not in model:
        model[sensor_id] = {"mean": 0, "variance": 0, "count": 0}
    
    stats = model[sensor_id]
    
    # Statistiche esistenti
    old_mean = stats["mean"]
    old_variance = stats["variance"]
    old_count = stats["count"]
    
    # Statistiche del nuovo chunk
    new_count = len(new_values)
    new_mean = sum(new_values) / new_count
    
    if new_count > 1:
        new_variance = sum((x - new_mean) ** 2 for x in new_values) / new_count
    else:
        new_variance = 0
    
    # Combinazione (Chan's parallel algorithm)
    total_count = old_count + new_count
    
    if total_count > 0:
        # Nuova media combinata
        combined_mean = (old_mean * old_count + new_mean * new_count) / total_count
        
        # Nuova varianza combinata
        delta = new_mean - old_mean
        M2_old = old_variance * old_count
        M2_new = new_variance * new_count
        M2_combined = M2_old + M2_new + (delta ** 2) * old_count * new_count / total_count
        combined_variance = M2_combined / total_count
        
        # Log dettagliato per debug
        print(f"      📈 Model Update [{sensor_id}]:")
        print(f"         Old: mean={old_mean:.2f}, var={old_variance:.4f}, n={old_count}")
        print(f"         Chunk: mean={new_mean:.2f}, var={new_variance:.4f}, n={new_count}")
        print(f"         Delta={delta:.2f}, M2_old={M2_old:.2f}, M2_new={M2_new:.2f}")
        print(f"         New: mean={combined_mean:.2f}, var={combined_variance:.4f}, n={total_count}")
        
        model[sensor_id] = {
            "mean": combined_mean,
            "variance": combined_variance,
            "count": total_count
        }
    
    return model


def filter_chunk_with_model(chunk_data, model, sigma_threshold=3.0):
    """
    Filtra un chunk usando le statistiche globali del modello.
    
    Args:
        chunk_data: lista di dizionari con i record del chunk
        model: dizionario del modello con statistiche per sensore
        sigma_threshold: soglia sigma per il filtro
    
    Returns:
        (clean_data, discarded_count): dati puliti e conteggio scartati
    """
    clean_data = []
    discarded_count = 0
    
    for record in chunk_data:
        sensor_id = record.get("sensor_id")
        temp = record.get("temp")
        
        if sensor_id is None or temp is None:
            discarded_count += 1
            continue
        
        # Se non abbiamo statistiche per questo sensore, accettiamo il dato
        if sensor_id not in model or model[sensor_id]["count"] < 10:
            clean_data.append(record)
            continue
        
        stats = model[sensor_id]
        mean = stats["mean"]
        std_dev = math.sqrt(stats["variance"]) if stats["variance"] > 0 else 0
        
        # Filtro 3-sigma
        if std_dev > 0:
            deviation = abs(temp - mean)
            threshold = sigma_threshold * std_dev
            
            if deviation <= threshold:
                clean_data.append(record)
            else:
                discarded_count += 1
        else:
            # Se la varianza è 0, accettiamo il dato
            clean_data.append(record)
    
    return clean_data, discarded_count


def get_model_age(spark):
    """
    Ritorna l'età del file del modello in secondi.
    Utile per capire se siamo in fase di calibrazione.
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
            mod_time = status.getModificationTime()  # ms
            current_time = time.time() * 1000  # ms
            return (current_time - mod_time) / 1000.0
    except Exception as e:
        print(f"Error checking model age: {e}")
    return -1


def clear_incoming_files(spark):
    """
    Elimina i file processati da /incoming per evitare duplicati.
    """
    try:
        sc = spark.sparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE),
            sc._jsc.hadoopConfiguration()
        )
        incoming_path = spark._jvm.org.apache.hadoop.fs.Path("/iot-data/incoming")
        
        if fs.exists(incoming_path):
            file_status_list = fs.listStatus(incoming_path)
            deleted_count = 0
            for file_status in file_status_list:
                file_path = file_status.getPath()
                if file_path.getName().endswith('.jsonl'):
                    fs.delete(file_path, False)
                    deleted_count += 1
            print(f"✅ Cleaned up {deleted_count} processed files from /incoming")
            return deleted_count
    except Exception as e:
        print(f"⚠️ Warning: Could not clear incoming files: {e}")
    return 0


def main():
    # Inizializzazione Spark
    spark = SparkSession.builder \
        .appName("LambdaBatchProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("Spark Batch Processor Starting (Stateful Chunk Mode)...")
    print(f"Chunk Size: {CHUNK_SIZE} | Sigma Threshold: {SIGMA_THRESHOLD}")
    print("=" * 60)
    
    # ==============================================================================
    # 1. CARICA MODELLO GLOBALE
    # ==============================================================================
    model = load_model(spark)
    
    if not model:
        print("⚠️ No model found. Will create initial statistics from this batch.")
    
    # ==============================================================================
    # 2. CARICA E ORDINA DATI
    # ==============================================================================
    
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("source", StringType(), True)
    ])
    
    try:
        df = spark.read.schema(schema).json(INPUT_PATH)
        total_count = df.count()
        print(f"📥 Loaded {total_count} records from HDFS")
        
        if total_count == 0:
            print("No data to process")
            spark.stop()
            return
    except Exception as e:
        print(f"Error loading data: {e}")
        spark.stop()
        return
    
    # Ordina per timestamp e aggiungi indice per chunking
    df_sorted = df.withColumn("ts_long", unix_timestamp(col("timestamp")))
    window_order = Window.partitionBy("sensor_id").orderBy("ts_long")
    df_indexed = df_sorted.withColumn("row_idx", row_number().over(window_order))
    df_indexed = df_indexed.withColumn("chunk_id", floor((col("row_idx") - 1) / CHUNK_SIZE))
    
    # ==============================================================================
    # 3. PROCESSA CHUNK PER CHUNK (STATEFUL)
    # ==============================================================================
    print(f"\n{'='*60}")
    print("CHUNK-BASED STATEFUL FILTERING")
    print(f"{'='*60}")
    
    # Converti in lista per processamento iterativo
    all_records = [row.asDict() for row in df_indexed.collect()]
    
    # Raggruppa per sensor_id e chunk_id
    from collections import defaultdict
    chunks_by_sensor = defaultdict(lambda: defaultdict(list))
    
    for record in all_records:
        sensor_id = record["sensor_id"]
        chunk_id = record["chunk_id"]
        chunks_by_sensor[sensor_id][chunk_id].append(record)
    
    # Statistiche globali
    total_clean = 0
    total_discarded = 0
    all_clean_records = []
    
    # Processa ogni sensore
    for sensor_id in sorted(chunks_by_sensor.keys()):
        sensor_chunks = chunks_by_sensor[sensor_id]
        print(f"\n📊 Processing sensor: {sensor_id}")
        
        sensor_clean = 0
        sensor_discarded = 0
        
        # Processa chunk in ordine
        for chunk_id in sorted(sensor_chunks.keys()):
            chunk_data = sensor_chunks[chunk_id]
            
            # Filtra usando il modello corrente
            clean_chunk, discarded = filter_chunk_with_model(chunk_data, model, SIGMA_THRESHOLD)
            
            # Aggiorna statistiche
            sensor_clean += len(clean_chunk)
            sensor_discarded += discarded
            all_clean_records.extend(clean_chunk)
            
            # Aggiorna modello con i dati puliti
            if clean_chunk:
                clean_values = [r["temp"] for r in clean_chunk]
                model = update_model_incremental(model, sensor_id, clean_values)
            
            # Log per chunk (opzionale, commenta se troppo verbose)
            if len(chunk_data) > 0:
                print(f"   Chunk {int(chunk_id)}: {len(chunk_data)} → {len(clean_chunk)} clean, {discarded} discarded")
        
        total_clean += sensor_clean
        total_discarded += sensor_discarded
        
        # Mostra statistiche aggiornate per sensore
        if sensor_id in model:
            s = model[sensor_id]
            print(f"   Updated model: mean={s['mean']:.2f}, std={math.sqrt(s['variance']):.4f}, n={s['count']}")
    
    print(f"\n{'='*60}")
    print(f"FILTERING COMPLETE:")
    print(f"  Total: {total_count} → Clean: {total_clean}, Discarded: {total_discarded}")
    print(f"{'='*60}")
    
    # ==============================================================================
    # 4. SALVA MODELLO AGGIORNATO
    # ==============================================================================
    save_model(spark, model)
    
    # ==============================================================================
    # 5. CALCOLO INDICATORI TECNICI (OHLC, RSI, Bollinger)
    # ==============================================================================
    
    if not all_clean_records:
        print("No clean data for technical indicators")
        clear_incoming_files(spark)
        spark.stop()
        return
    
    # Riconverti in DataFrame per calcoli Spark
    # Estrai solo i campi necessari per evitare problemi di inferenza schema
    clean_records_filtered = [
        {
            "sensor_id": r.get("sensor_id"),
            "timestamp": r.get("timestamp"),
            "temp": float(r.get("temp")) if r.get("temp") is not None else None,
            "source": r.get("source")
        }
        for r in all_clean_records
    ]
    df_clean = spark.createDataFrame(clean_records_filtered, schema=schema)
    df_clean = df_clean.withColumn("ts_long", unix_timestamp(col("timestamp")))
    
    # Finestre per indicatori
    window_spec = Window.partitionBy("sensor_id").orderBy("ts_long")
    window_rsi = Window.partitionBy("sensor_id").orderBy("ts_long").rowsBetween(-14, 0)
    window_bb = Window.partitionBy("sensor_id").orderBy("ts_long").rowsBetween(-20, 0)
    
    # Calcolo Variazioni prezzo
    df_calc = df_clean.withColumn(
        "price_change", col("temp") - lag("temp", 1).over(window_spec)
    ).withColumn(
        "gain", when(col("price_change") > 0, col("price_change")).otherwise(0)
    ).withColumn(
        "loss", when(col("price_change") < 0, -col("price_change")).otherwise(0)
    ).withColumn(
        "price_10_ago", lag("temp", 10).over(window_spec)
    )
    
    # Calcolo RSI
    df_calc = df_calc.withColumn("avg_gain", avg("gain").over(window_rsi)) \
                     .withColumn("avg_loss", avg("loss").over(window_rsi)) \
                     .withColumn("rs", when(col("avg_loss") > 0, col("avg_gain") / col("avg_loss")).otherwise(100)) \
                     .withColumn("rsi", 100 - (100 / (1 + col("rs"))))
    
    # Calcolo Bollinger Bands
    df_calc = df_calc.withColumn("sma_20", avg("temp").over(window_bb)) \
                     .withColumn("std_20", stddev("temp").over(window_bb)) \
                     .withColumn("bollinger_upper", col("sma_20") + 2 * col("std_20")) \
                     .withColumn("bollinger_lower", col("sma_20") - 2 * col("std_20"))
    
    # Calcolo Momentum
    df_calc = df_calc.withColumn("momentum", 
        when(col("price_10_ago") > 0, 
             ((col("temp") - col("price_10_ago")) / col("price_10_ago")) * 100
        ).otherwise(0)
    )
    
    # Aggregazione Finale (OHLC giornaliero)
    result = df_calc.groupBy("sensor_id").agg(
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
    
    # Aggiunta metriche derivate
    result = result.withColumn("daily_change", col("close") - col("open")) \
                   .withColumn("daily_change_pct", when(col("open") > 0, (col("daily_change") / col("open")) * 100).otherwise(0)) \
                   .withColumn("processed_at", lit(datetime.utcnow().isoformat() + "Z"))
    
    print("\nBatch Processing Results:")
    result.show(truncate=False)
    
    # ==============================================================================
    # 6. SALVATAGGIO INCREMENTALE (OHLC Output)
    # ==============================================================================
    
    today = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{OUTPUT_PATH}/date={today}"
    
    # Check Calibrazione
    is_calibration_run = False
    try:
        sc = spark.sparkContext
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE), sc._jsc.hadoopConfiguration()
        )
        if not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(output_path)):
            if 0 <= get_model_age(spark) < 120:
                is_calibration_run = True
                print("📋 Calibration run detected (Model < 120s old)")
    except Exception as e:
        print(f"Calibration check error: {e}")

    # Gestione Incrementale delle statistiche
    existing_data = {}
    save_safe = False
    
    if not is_calibration_run:
        try:
            try:
                prev_df = spark.read.json(output_path)
                rows = prev_df.collect()
                for r in rows:
                    sid = r["sensor_id"]
                    if sid:
                        existing_data[sid] = {
                            "count": r["count"] if r["count"] else 0,
                            "mean": r["mean"] if r["mean"] else 0,
                            "min": r["min"] if r["min"] else float('inf'),
                            "max": r["max"] if r["max"] else float('-inf'),
                            "volatility": r["volatility"] if r["volatility"] else 0,
                            "open": r["open"] if r["open"] else 0,
                            "close": r["close"] if r["close"] else 0,
                            "avg_rsi": r["avg_rsi"] if r["avg_rsi"] else 50,
                            "avg_momentum": r["avg_momentum"] if r["avg_momentum"] else 0,
                            "bollinger_upper": r["bollinger_upper"] if r["bollinger_upper"] else None,
                            "bollinger_lower": r["bollinger_lower"] if r["bollinger_lower"] else None,
                            "daily_change": r["daily_change"] if r["daily_change"] else 0,
                            "daily_change_pct": r["daily_change_pct"] if r["daily_change_pct"] else 0
                        }
                print(f"✅ Loaded existing data for sensors: {list(existing_data.keys())}")
            except Exception:
                print("No existing valid data found for today (Starting fresh).")
            
            # Merge incrementale
            if existing_data:
                res_list = [row.asDict() for row in result.collect()]
                current_sensor_ids = {row.get("sensor_id") for row in res_list}
                
                # Merge dati esistenti con nuovi
                for row in res_list:
                    sid = row.get("sensor_id")
                    if sid in existing_data:
                        old = existing_data[sid]
                        old_count = old["count"]
                        new_count = row["count"]
                        total_count_merged = old_count + new_count
                        
                        if total_count_merged > 0:
                            row["mean"] = (old["mean"] * old_count + row["mean"] * new_count) / total_count_merged
                            
                            import builtins
                            row["min"] = builtins.min(old["min"], row["min"]) if row["min"] else old["min"]
                            row["max"] = builtins.max(old["max"], row["max"]) if row["max"] else old["max"]
                            
                            if old["volatility"] and row["volatility"]:
                                row["volatility"] = (old["volatility"] * old_count + row["volatility"] * new_count) / total_count_merged
                            
                            if old["avg_rsi"] and row["avg_rsi"]:
                                row["avg_rsi"] = (old["avg_rsi"] * old_count + row["avg_rsi"] * new_count) / total_count_merged
                            if old["avg_momentum"] and row["avg_momentum"]:
                                row["avg_momentum"] = (old["avg_momentum"] * old_count + row["avg_momentum"] * new_count) / total_count_merged
                            
                            row["open"] = old["open"]
                            row["count"] = total_count_merged
                            row["daily_change"] = row["close"] - row["open"]
                            if row["open"] > 0:
                                row["daily_change_pct"] = (row["daily_change"] / row["open"]) * 100
                
                # Aggiungi sensori esistenti che non sono nel batch corrente (preserva storico)
                for sid, old in existing_data.items():
                    if sid not in current_sensor_ids:
                        # Mantieni i dati precedenti senza modifiche
                        preserved_row = {
                            "sensor_id": sid,
                            "open": old["open"],
                            "close": old.get("close", old["open"]),
                            "min": old["min"],
                            "max": old["max"],
                            "mean": old["mean"],
                            "volatility": old["volatility"],
                            "count": old["count"],
                            "avg_rsi": old["avg_rsi"],
                            "bollinger_upper": old.get("bollinger_upper"),
                            "bollinger_lower": old.get("bollinger_lower"),
                            "avg_momentum": old["avg_momentum"],
                            "daily_change": old.get("daily_change", 0),
                            "daily_change_pct": old.get("daily_change_pct", 0),
                            "processed_at": datetime.utcnow().isoformat() + "Z"
                        }
                        res_list.append(preserved_row)
                        print(f"   ⚠️ Preserved existing data for sensor {sid} (no new data in batch)")
                
                result = spark.createDataFrame(res_list)
                print("✅ Updated stats with cumulative weighted averages")
            
            save_safe = True
        except Exception as e:
            print(f"❌ Error merging stats: {e}")
            import traceback
            traceback.print_exc()
    else:
        save_safe = True

    if save_safe:
        result.coalesce(1).write.mode("overwrite").json(output_path)
        print(f"Results saved to: {output_path}")

    # ==============================================================================
    # 7. ARCHIVIAZIONE E PULIZIA
    # ==============================================================================
    
    archive_dest = f"{ARCHIVE_PATH}/date={today}"
    try:
        # Crea DataFrame per archiviazione (senza colonne temporanee)
        archive_cols = ["sensor_id", "timestamp", "temp", "source"]
        df_archive = spark.createDataFrame([{k: r[k] for k in archive_cols if k in r} for r in all_clean_records])
        df_archive.coalesce(1).write.mode("append").json(archive_dest)
        print(f"Data archived to: {archive_dest}")
        clear_incoming_files(spark)
    except Exception as e:
        print(f"Error archiving data: {e}")

    # ==============================================================================
    # 8. AGGIORNAMENTO STATISTICHE GLOBALI (stats.json)
    # ==============================================================================
    # Nota: scriviamo stats SEMPRE, anche durante calibrazione
    
    stats_file = f"/iot-stats/daily-aggregate/stats_{today}.json"
    current_stats = {"total_clean": 0, "total_discarded": 0, "total_processed": 0}
    
    try:
        # Crea connessione HDFS per stats (non dipende da fs precedente)
        sc = spark.sparkContext
        stats_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(HDFS_NAMENODE), sc._jsc.hadoopConfiguration()
        )
        
        # Leggi stats esistenti se presenti
        stats_path = spark._jvm.org.apache.hadoop.fs.Path(stats_file)
        if stats_fs.exists(stats_path):
            stream = stats_fs.open(stats_path)
            reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(stream))
            content = reader.readLine()
            if content:
                current_stats = json.loads(content)
            reader.close()
            stream.close()
    except Exception as e:
        print(f"⚠️ Could not read existing stats: {e}")

    # Calcola nuovi totali (INCREMENTALI)
    # total_clean: somma dei dati puliti di questo batch + precedenti
    # total_discarded: somma dei dati scartati di questo batch + precedenti
    new_clean_total = current_stats["total_clean"] + total_clean
    new_discarded_total = current_stats["total_discarded"] + total_discarded
    
    final_stats = {
        "total_clean": new_clean_total,
        "total_discarded": new_discarded_total,
        "total_processed": new_clean_total + new_discarded_total
    }
    
    try:
        # Crea directory se non esiste
        stats_dir = spark._jvm.org.apache.hadoop.fs.Path("/iot-stats/daily-aggregate")
        if not stats_fs.exists(stats_dir):
            stats_fs.mkdirs(stats_dir)
        
        # Scrivi stats
        stats_path = spark._jvm.org.apache.hadoop.fs.Path(stats_file)
        out = stats_fs.create(stats_path, True)
        out.write(json.dumps(final_stats).encode('utf-8'))
        out.close()
        print(f"📊 Stats updated: {final_stats}")
    except Exception as e:
        print(f"Error writing stats: {e}")

    spark.stop()

if __name__ == "__main__":
    main()