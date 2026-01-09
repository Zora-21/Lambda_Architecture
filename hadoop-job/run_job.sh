#!/bin/bash

# --- CONFIGURAZIONE ---
source /opt/hadoop-3.2.1/etc/hadoop/hadoop-env.sh
export HADOOP_HOME=/opt/hadoop-3.2.1
HDFS_CMD="$HADOOP_HOME/bin/hdfs"
HADOOP_CMD="$HADOOP_HOME/bin/hadoop"
HDFS_URI="hdfs://namenode:9000"

# Cartelle
INCOMING_DIR="/iot-data/incoming"
ARCHIVE_DIR_BASE="/iot-data/archive"
INCREMENTAL_OUT="/iot-output/incremental"
DAILY_SUMMARY_DIR="/iot-stats/daily-summary"
AGGREGATE_STATS_DIR="/iot-stats/daily-aggregate"

MODEL_FILE_HDFS="/models/model.json"
MODEL_LOCAL="/app/model.json"

TODAY_DATE=$(date +%Y-%m-%d)
YESTERDAY_DATE=$(date -d "yesterday" +%Y-%m-%d)
CURRENT_TIME=$(date +%H-%M-%S)

log() { echo "$(date +'%Y-%m-%d %H:%M:%S') - $1"; }

# --- 0. CHECK MICRO-BATCH ---
if ! $HDFS_CMD dfs -fs $HDFS_URI -test -e "$INCOMING_DIR/*.jsonl"; then
    exit 0
fi

log "🚀 Avvio Micro-Batch $CURRENT_TIME su dati nuovi..."

# --- CHECK: Il modello esiste già? ---
MODEL_EXISTED_BEFORE=false
if $HDFS_CMD dfs -fs $HDFS_URI -test -e "$MODEL_FILE_HDFS" 2>/dev/null; then
    MODEL_EXISTED_BEFORE=true
fi

# --- FASE 1: TRAINING ---
{
  $HDFS_CMD dfs -fs $HDFS_URI -cat "$ARCHIVE_DIR_BASE/date=$YESTERDAY_DATE/*.jsonl" 2>/dev/null
  $HDFS_CMD dfs -fs $HDFS_URI -cat "$ARCHIVE_DIR_BASE/date=$TODAY_DATE/*.jsonl" 2>/dev/null
  $HDFS_CMD dfs -fs $HDFS_URI -cat "$INCOMING_DIR/*.jsonl" 2>/dev/null
} | python3 /app/train_model.py > $MODEL_LOCAL

if [ -s "$MODEL_LOCAL" ]; then
    $HDFS_CMD dfs -fs $HDFS_URI -put -f $MODEL_LOCAL $MODEL_FILE_HDFS
fi

# --- SKIP: Se il modello non esisteva, salta MapReduce e metriche ---
# I dati verrebbero processati senza pulizia, invalidando le statistiche
if [ "$MODEL_EXISTED_BEFORE" = false ]; then
    log "⏭️ Primo avvio: Modello creato, metriche saltate (dati non puliti)"
    
    # Archivia comunque i dati incoming
    DEST_ARCHIVE="$ARCHIVE_DIR_BASE/date=$TODAY_DATE"
    $HDFS_CMD dfs -fs $HDFS_URI -mkdir -p $DEST_ARCHIVE
    for file in $($HDFS_CMD dfs -fs $HDFS_URI -ls "$INCOMING_DIR/*.jsonl" | awk '{print $8}'); do
        $HDFS_CMD dfs -fs $HDFS_URI -mv "$file" "$DEST_ARCHIVE/"
    done
    
    log "✅ Calibrazione completata. Le prossime esecuzioni calcoleranno metriche pulite."
    exit 0
fi

# --- FASE 2: MAPREDUCE (SOLO SU INCOMING) ---
BATCH_OUTPUT_DIR="$INCREMENTAL_OUT/date=$TODAY_DATE/batch_$CURRENT_TIME"

$HADOOP_CMD jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapred.job.name="MicroBatch $CURRENT_TIME" \
    -D mapreduce.job.reduces=3 \
    -fs $HDFS_URI \
    -files /app/mapper.py,/app/reducer.py,$MODEL_LOCAL \
    -mapper "python3 /app/mapper.py" \
    -reducer "python3 /app/reducer.py" \
    -input "$INCOMING_DIR/*.jsonl" \
    -output "$BATCH_OUTPUT_DIR" > /dev/null 2>&1

# --- VARIABILE PER TUTTI I RISULTATI DI OGGI ---
ALL_BATCHES_RESULTS="$INCREMENTAL_OUT/date=$TODAY_DATE/*/part-*"

# --- FASE 3: UNIFICAZIONE METRICHE (FIXED) ---
log "∑ Unificazione risultati giornalieri..."
SUMMARY_OUTPUT_PATH="$DAILY_SUMMARY_DIR/date=$TODAY_DATE"
$HDFS_CMD dfs -fs $HDFS_URI -mkdir -p $SUMMARY_OUTPUT_PATH

if $HDFS_CMD dfs -fs $HDFS_URI -test -e "$ALL_BATCHES_RESULTS"; then
    # --- FIX IMPORTANTE: Scrittura diretta su file (No Variable Capture) ---
    # Usiamo python3 -u per evitare buffering e > per scrivere su disco
    $HDFS_CMD dfs -fs $HDFS_URI -cat "$ALL_BATCHES_RESULTS" | python3 -u /app/unify_batches.py > /tmp/daily_unified.json
    
    # Controlla se il file locale non è vuoto (-s)
    if [ -s /tmp/daily_unified.json ]; then
        $HDFS_CMD dfs -fs $HDFS_URI -put -f /tmp/daily_unified.json "$SUMMARY_OUTPUT_PATH/daily_stats.json"
        log "✅ Daily Stats generate."
    else
        log "⚠️ Daily Stats vuote (Errore Python o Input vuoto)."
    fi
fi

# --- FASE 3.5: AGGREGAZIONE STATISTICHE (FIXED) ---
STATS_OUTPUT_PATH="$AGGREGATE_STATS_DIR/date=$TODAY_DATE"
$HDFS_CMD dfs -fs $HDFS_URI -mkdir -p $STATS_OUTPUT_PATH

if $HDFS_CMD dfs -fs $HDFS_URI -test -e "$ALL_BATCHES_RESULTS"; then
    # Anche qui, scrittura diretta
    $HDFS_CMD dfs -fs $HDFS_URI -cat "$ALL_BATCHES_RESULTS" | python3 -u /app/aggregate_stats.py > /tmp/agg.json
    
    if [ -s /tmp/agg.json ]; then
        $HDFS_CMD dfs -fs $HDFS_URI -put -f /tmp/agg.json "$STATS_OUTPUT_PATH/aggregate_stats.json"
        log "✅ Aggregate Stats generate."
    fi
fi

# --- FASE 4: ARCHIVIAZIONE ---
DEST_ARCHIVE="$ARCHIVE_DIR_BASE/date=$TODAY_DATE"
$HDFS_CMD dfs -fs $HDFS_URI -mkdir -p $DEST_ARCHIVE

for file in $($HDFS_CMD dfs -fs $HDFS_URI -ls "$INCOMING_DIR/*.jsonl" | awk '{print $8}'); do
    $HDFS_CMD dfs -fs $HDFS_URI -mv "$file" "$DEST_ARCHIVE/"
done

log "✅ Micro-Batch completato e archiviato."