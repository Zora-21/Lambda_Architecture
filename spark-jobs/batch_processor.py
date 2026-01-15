#!/usr/bin/env python3
"""
Spark Batch Processor - HDFS Data Processing (Optimized)

Legge dati da HDFS, applica un filtro adattivo basato su Window Functions
(Z-Score su finestra mobile) per gestire la volatilità delle crypto,
calcola metriche OHLC/RSI/Bollinger e aggiorna le statistiche.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, first, last, min, max, avg, stddev, count,
    lag, when, lit, sum as spark_sum, unix_timestamp, abs
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
import time
from datetime import datetime

# --- Configurazione ---
HDFS_NAMENODE = "hdfs://namenode:9000"
INPUT_PATH = f"{HDFS_NAMENODE}/iot-data/incoming/*.jsonl"
OUTPUT_PATH = f"{HDFS_NAMENODE}/iot-output/spark"
ARCHIVE_PATH = f"{HDFS_NAMENODE}/iot-data/archive"
MODEL_PATH = f"{HDFS_NAMENODE}/models/model.json"
STATS_FILE_PATH = "/iot-stats/daily-aggregate"

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
            mod_time = status.getModificationTime() # ms
            current_time = time.time() * 1000 # ms
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
    print("Spark Batch Processor Starting (Smart Window Mode)...")
    print("=" * 60)
    
    # Definizione Schema
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("source", StringType(), True)
    ])
    
    # Caricamento Dati
    try:
        df = spark.read.schema(schema).json(INPUT_PATH)
        total_count = df.count()
        print(f"Loaded {total_count} records from HDFS")
        
        if total_count == 0:
            print("No data to process")
            spark.stop()
            return
    except Exception as e:
        print(f"Error loading data: {e}")
        spark.stop()
        return

    # ==============================================================================
    # 1. SMART FILTERING: ROLLING Z-SCORE (Gestione Oscillazioni)
    # ==============================================================================
    print("Applying Smart Rolling Filter (Adaptive to volatility)...")
    
    # Aggiunge timestamp numerico per ordinamento corretto
    df_sorted = df.withColumn("ts_long", unix_timestamp(col("timestamp")))
    
    # Definizione Finestra: Partition per Sensore, Ordine Temporale, Ultimi 20 record
    # Questa finestra "guarda indietro" per stabilire il contesto locale
    window_rolling = Window.partitionBy("sensor_id").orderBy("ts_long").rowsBetween(-20, -1)
    
    # Calcola Statistiche Mobili (Rolling Stats)
    df_stats = df_sorted.withColumn("rolling_mean", avg("temp").over(window_rolling)) \
                        .withColumn("rolling_std", stddev("temp").over(window_rolling))
    
    # Gestione casi limite (primi record o volatilità zero)
    # Se stddev è null o 0, usiamo un valore minimo per evitare errori
    df_stats = df_stats.fillna(0, subset=["rolling_std"])
    df_stats = df_stats.withColumn("rolling_std", 
                                   when(col("rolling_std") == 0, 0.001)
                                   .otherwise(col("rolling_std")))
    
    # Parametro Sigma (Soglia di tolleranza)
    # 3 Sigma copre il 99.7% dei casi in una distribuzione normale.
    # Se il prezzo esce da questa banda, è un'anomalia statistica locale.
    SIGMA_THRESHOLD = 3.0
    
    # Filtro Effettivo
    df_filtered = df_stats.filter(
        (col("rolling_mean").isNull()) |  # Accetta i primi record (warmup)
        (abs(col("temp") - col("rolling_mean")) <= (SIGMA_THRESHOLD * col("rolling_std")))
    )
    
    clean_count = df_filtered.count()
    discarded_count = total_count - clean_count
    
    print(f"Filtering Complete:")
    print(f" -> Total: {total_count}")
    print(f" -> Clean: {clean_count}")
    print(f" -> Discarded: {discarded_count}")
    
    # ==============================================================================
    # 2. CALCOLO INDICATORI TECNICI (OHLC, RSI, Bollinger)
    # ==============================================================================
    
    # Usa df_filtered per i calcoli successivi
    df_calc = df_filtered
    
    # Finestre per indicatori
    window_spec = Window.partitionBy("sensor_id").orderBy("ts_long")
    window_rsi = Window.partitionBy("sensor_id").orderBy("ts_long").rowsBetween(-14, 0)
    window_bb = Window.partitionBy("sensor_id").orderBy("ts_long").rowsBetween(-20, 0)
    
    # Calcolo Variazioni prezzo
    df_calc = df_calc.withColumn(
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
    
    # Calcolo Bollinger Bands (per visualizzazione dashboard)
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
    # 3. SALVATAGGIO INCREMENTALE (OHLC Output)
    # ==============================================================================
    
    today = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{OUTPUT_PATH}/date={today}"
    
    # Check Calibrazione (per non resettare i conteggi se è il primo run "vero")
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
    existing_data = {}  # {sensor_id: {count, mean, min, max, volatility, ...}}
    save_safe = False
    
    if not is_calibration_run:
        try:
            # Tenta di leggere i dati precedenti
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
                            "open": r["open"] if r["open"] else 0,  # Manteniamo l'open originale
                            "avg_rsi": r["avg_rsi"] if r["avg_rsi"] else 50,
                            "avg_momentum": r["avg_momentum"] if r["avg_momentum"] else 0
                        }
                print(f"✅ Loaded existing data for sensors: {list(existing_data.keys())}")
            except Exception:
                print("No existing valid data found for today (Starting fresh).")
            
            # Calcola statistiche cumulative pesate
            if existing_data:
                res_list = [row.asDict() for row in result.collect()]
                for row in res_list:
                    sid = row.get("sensor_id")
                    if sid in existing_data:
                        old = existing_data[sid]
                        old_count = old["count"]
                        new_count = row["count"]
                        total_count = old_count + new_count
                        
                        if total_count > 0:
                            # Media pesata cumulativa
                            row["mean"] = (old["mean"] * old_count + row["mean"] * new_count) / total_count
                            
                            # Min/Max globali
                            import builtins
                            row["min"] = builtins.min(old["min"], row["min"]) if row["min"] else old["min"]
                            row["max"] = builtins.max(old["max"], row["max"]) if row["max"] else old["max"]
                            
                            # Volatilità pesata (approssimazione)
                            if old["volatility"] and row["volatility"]:
                                row["volatility"] = (old["volatility"] * old_count + row["volatility"] * new_count) / total_count
                            
                            # RSI e Momentum pesati
                            if old["avg_rsi"] and row["avg_rsi"]:
                                row["avg_rsi"] = (old["avg_rsi"] * old_count + row["avg_rsi"] * new_count) / total_count
                            if old["avg_momentum"] and row["avg_momentum"]:
                                row["avg_momentum"] = (old["avg_momentum"] * old_count + row["avg_momentum"] * new_count) / total_count
                            
                            # Open rimane quello del primo batch del giorno
                            row["open"] = old["open"]
                            
                            # Count cumulativo
                            row["count"] = total_count
                            
                            # Ricalcola daily_change con il nuovo close e vecchio open
                            row["daily_change"] = row["close"] - row["open"]
                            if row["open"] > 0:
                                row["daily_change_pct"] = (row["daily_change"] / row["open"]) * 100
                
                result = spark.createDataFrame(res_list)
                print("✅ Updated stats with cumulative weighted averages")
            
            save_safe = True
        except Exception as e:
            print(f"❌ Error merging stats: {e}")
            import traceback
            traceback.print_exc()
    else:
        save_safe = True  # In calibrazione sovrascriviamo senza problemi

    if save_safe:
        result.coalesce(1).write.mode("overwrite").json(output_path)
        print(f"Results saved to: {output_path}")

    # ==============================================================================
    # 4. ARCHIVIAZIONE E PULIZIA
    # ==============================================================================
    
    archive_dest = f"{ARCHIVE_PATH}/date={today}"
    try:
        df_filtered.drop("ts_long", "rolling_mean", "rolling_std").coalesce(1).write.mode("append").json(archive_dest)
        print(f"Data archived to: {archive_dest}")
        # Solo se archiviazione OK puliamo l'incoming
        clear_incoming_files(spark)
    except Exception as e:
        print(f"Error archiving data: {e}")

    # ==============================================================================
    # 5. AGGIORNAMENTO STATISTICHE GLOBALI (stats.json)
    # ==============================================================================
    
    if not is_calibration_run:
        stats_file = f"/iot-stats/daily-aggregate/stats_{today}.json"
        current_stats = {"total_clean": 0, "total_discarded": 0, "total_processed": 0}
        
        # Leggi esistente
        try:
            path = spark._jvm.org.apache.hadoop.fs.Path(stats_file)
            if fs.exists(path):
                stream = fs.open(path)
                reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(stream))
                content = reader.readLine()
                if content:
                    current_stats = json.loads(content)
                reader.close()
                stream.close()
        except Exception:
            pass

        # Calcola nuovi totali (Clean deve prendere il totale cumulativo OHLC)
        try:
            # Totale Clean = Somma dei count nel dataframe OHLC (che sono già cumulativi)
            new_clean_total = sum(row["count"] for row in result.collect())
        except:
            new_clean_total = current_stats["total_clean"] + clean_count
            
        # Totale Discarded = Cumulativo (Precedente + Attuale)
        new_discarded_total = current_stats["total_discarded"] + discarded_count
        
        final_stats = {
            "total_clean": new_clean_total,
            "total_discarded": new_discarded_total,
            "total_processed": new_clean_total + new_discarded_total
        }
        
        # Scrivi stats
        try:
            if not fs.exists(spark._jvm.org.apache.hadoop.fs.Path("/iot-stats/daily-aggregate")):
                fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path("/iot-stats/daily-aggregate"))
            
            out = fs.create(spark._jvm.org.apache.hadoop.fs.Path(stats_file), True)
            out.write(json.dumps(final_stats).encode('utf-8'))
            out.close()
            print(f"📊 Stats updated: {final_stats}")
        except Exception as e:
            print(f"Error writing stats: {e}")

    spark.stop()

if __name__ == "__main__":
    main()