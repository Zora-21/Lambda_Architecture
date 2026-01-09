CREATE KEYSPACE IF NOT EXISTS iot_keyspace
WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2 };

USE iot_keyspace;

CREATE TABLE IF NOT EXISTS sensor_data (
  sensor_id TEXT,
  timestamp TIMESTAMP,
  temp FLOAT,
  PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);