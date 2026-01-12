#!/usr/bin/env python3
"""
Spark Model Trainer - Anomaly Detection Model

Calcola statistiche (mean, std_dev) per ogni sensor_id dai dati
storici e salva il modello su HDFS.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json
from datetime import datetime, timedelta

# Configuration
HDFS_NAMENODE = "hdfs://namenode:9000"
MODEL_PATH = "/models/model.json"


def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("LambdaModelTrainer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("Model Trainer Starting...")
    print("=" * 60)
    
    # Define schema
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("source", StringType(), True)
    ])
    
    # Time window: last 60 minutes for better variance capture
    window_minutes = 60
    cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
    cutoff_str = cutoff_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    print(f"Training window: last {window_minutes} minutes (since {cutoff_str})")
    
    # Build list of data sources (only recent data matters)
    today = datetime.now().strftime("%Y-%m-%d")
    
    sources = [
        f"{HDFS_NAMENODE}/iot-data/incoming/*.jsonl",
        f"{HDFS_NAMENODE}/iot-data/archive/date={today}/*.jsonl"
    ]
    
    # Load all available data
    all_dfs = []
    total_records = 0
    
    for source in sources:
        try:
            df = spark.read.schema(schema).json(source)
            # Filter by timestamp - only last 5 minutes
            df_filtered = df.filter(col("timestamp") >= cutoff_str)
            count_val = df_filtered.count()
            if count_val > 0:
                all_dfs.append(df_filtered)
                total_records += count_val
                print(f"Loaded {count_val} records from {source} (filtered)")
        except Exception as e:
            print(f"Could not load {source}: {e}")
    
    if not all_dfs:
        print("No data available for model training (in last 5 minutes)")
        spark.stop()
        return
    
    # Union all dataframes
    combined_df = all_dfs[0]
    for df in all_dfs[1:]:
        combined_df = combined_df.union(df)
    
    print(f"\nTotal records for training (last {window_minutes}min): {total_records}")
    
    # Calculate statistics per sensor
    stats = combined_df.groupBy("sensor_id").agg(
        avg("temp").alias("mean"),
        stddev("temp").alias("std_dev"),
        count("*").alias("count")
    )
    
    print("\nCalculated Statistics:")
    stats.show(truncate=False)
    
    # Build model dictionary
    model = {}
    for row in stats.collect():
        sensor_id = row["sensor_id"]
        mean = row["mean"] if row["mean"] is not None else 0
        std_dev = row["std_dev"] if row["std_dev"] is not None else 0
        
        count_val = row["count"] if row["count"] is not None else 0
        
        model[sensor_id] = {
            "mean": round(mean, 4),
            "std_dev": round(std_dev, 4),
            "count": int(count_val)  # Include count for EMA initialization
        }
    
    print(f"\nGenerated model for {len(model)} sensors:")
    for sensor_id, values in model.items():
        print(f"  {sensor_id}: mean={values['mean']:.2f}, std_dev={values['std_dev']:.2f}")
    
    # Save model to HDFS
    model_json = json.dumps(model)
    
    try:
        # Use RDD to write single file
        model_rdd = spark.sparkContext.parallelize([model_json])
        
        # Write to temp path first
        temp_path = f"{HDFS_NAMENODE}{MODEL_PATH}.tmp"
        final_path = f"{HDFS_NAMENODE}{MODEL_PATH}"
        
        # Delete temp if exists
        try:
            spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(HDFS_NAMENODE),
                spark.sparkContext._jsc.hadoopConfiguration()
            )
            temp_hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(temp_path)
            final_hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(final_path)
            
            if fs.exists(temp_hadoop_path):
                fs.delete(temp_hadoop_path, True)
            if fs.exists(final_hadoop_path):
                fs.delete(final_hadoop_path, False)
        except Exception:
            pass
        
        # Save as text file
        model_rdd.coalesce(1).saveAsTextFile(temp_path)
        
        # Rename part file to final name
        try:
            part_path = spark._jvm.org.apache.hadoop.fs.Path(f"{temp_path}/part-00000")
            if fs.exists(part_path):
                fs.rename(part_path, final_hadoop_path)
                fs.delete(temp_hadoop_path, True)
                print(f"\nModel saved to: {MODEL_PATH}")
        except Exception as e:
            print(f"Note: Model saved as directory: {temp_path}")
        
    except Exception as e:
        print(f"Error saving model: {e}")
        # Fallback: save via HTTP
        try:
            import requests
            url = f"http://namenode:9870/webhdfs/v1{MODEL_PATH}?op=CREATE&overwrite=true"
            resp = requests.put(url, allow_redirects=False, timeout=30)
            if resp.status_code == 307:
                redirect_url = resp.headers['Location']
                requests.put(redirect_url, data=model_json.encode('utf-8'), timeout=60)
                print(f"Model saved via WebHDFS: {MODEL_PATH}")
        except Exception as e2:
            print(f"Fallback save also failed: {e2}")
    
    print("\n" + "=" * 60)
    print("Model Training Complete!")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
