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
from datetime import datetime

# Configuration
HDFS_NAMENODE = "hdfs://namenode:9000"
INPUT_PATH = f"{HDFS_NAMENODE}/iot-data/incoming/*.jsonl"
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
    
    # Save results
    today = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{OUTPUT_PATH}/date={today}"
    
    try:
        result.coalesce(1).write.mode("overwrite").json(output_path)
        print(f"Results saved to: {output_path}")
    except Exception as e:
        print(f"Error saving results: {e}")
    
    # Archive processed data
    archive_path = f"{ARCHIVE_PATH}/date={today}"
    try:
        df_filtered.coalesce(1).write.mode("append").json(archive_path)
        print(f"Data archived to: {archive_path}")
    except Exception as e:
        print(f"Error archiving data: {e}")
    
    # --- Write aggregate stats (like old project's aggregate_stats.py) ---
    aggregate_stats = {
        "total_clean": clean_count,
        "total_discarded": discarded_count,
        "total_processed": total_count
    }
    
    stats_path = f"{AGGREGATE_STATS_PATH}/date={today}"
    try:
        # Create a DataFrame with a single row and write as JSON
        stats_df = spark.createDataFrame([aggregate_stats])
        stats_df.coalesce(1).write.mode("overwrite").json(stats_path)
        print(f"Aggregate stats saved to: {stats_path}")
        print(f"  -> Total: {total_count}, Clean: {clean_count}, Discarded: {discarded_count}")
    except Exception as e:
        print(f"Error saving aggregate stats: {e}")
    
    print("\n" + "=" * 60)
    print("Batch Processing Complete!")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
