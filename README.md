# Lambda Architecture - Crypto Price Analysis

Sistema di analisi real-time e batch per prezzi di criptovalute (BTC, ETH, SOL) basato su **Lambda Architecture**.

## 🏗️ Architettura

```
                    ┌─────────────────┐
                    │   DATA SOURCE   │
                    │  (Binance API)  │
                    │  BTC, ETH, SOL  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     KAFKA       │
                    │ Message Broker  │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌─────────────────┐           ┌─────────────────┐
    │   BATCH LAYER   │           │   SPEED LAYER   │
    │                 │           │                 │
    │  • HDFS Storage │           │  • Cassandra    │
    │  • Spark Jobs   │           │  • Real-time    │
    │  • Model Train  │           │  • Anomaly Det. │
    └────────┬────────┘           └────────┬────────┘
             │                             │
             └──────────────┬──────────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │  SERVING LAYER  │
                   │  Flask Dashboard│
                   │   localhost:5000│
                   └─────────────────┘
```

## 📦 Componenti

| Componente | Descrizione |
|------------|-------------|
| `iot-producer/` | Producer Kafka che legge prezzi da Binance WebSocket |
| `kafka-consumers/` | Consumer per Batch Layer (HDFS) e Speed Layer (Cassandra) |
| `spark-jobs/` | Job Spark: `batch_processor.py` e `model_trainer.py` |
| `spark-scheduler/` | Orchestratore che esegue job Spark periodicamente |
| `dashboard/` | Dashboard Flask con grafici real-time |
| `cassandra-config/` | Configurazione iniziale Cassandra |

## 🚀 Quick Start

### 1. Avvia il sistema
```bash
docker compose up -d
```

### 2. Accedi alla Dashboard
Apri [http://localhost:5000](http://localhost:5000)

### 3. Monitora i servizi
```bash
# Logs di tutti i servizi
docker compose logs -f

# Stato dei container
docker compose ps
```

### 4. Stop del sistema
```bash
docker compose down
```

## 🔧 Servizi Docker

| Servizio | Porta | Descrizione |
|----------|-------|-------------|
| `dashboard` | 5000 | Dashboard Flask |
| `spark-master` | 8080 | Spark Web UI |
| `namenode` | 9870 | HDFS Web UI |
| `kafka` | 9092, 29092 | Kafka Broker |
| `cassandra-seed` | 9042 | Cassandra DB |
| `zookeeper` | 2181 | Zookeeper |

## 📊 Sensori/Criptovalute

| Sensor ID | Simbolo | Criptovaluta |
|-----------|---------|--------------|
| A1 | BTC-USD | Bitcoin |
| B1 | ETH-USD | Ethereum |
| C1 | SOL-USD | Solana |

## 🔄 Flusso Dati

1. **Producer** → Legge prezzi da Binance, invia a Kafka
2. **Batch Consumer** → Scrive su HDFS (`/iot-data/incoming/`)
3. **Speed Consumer** → Scrive su Cassandra, rileva anomalie
4. **Spark Scheduler** → Ogni 5 min esegue:
   - `model_trainer.py` → Aggiorna modello statistico
   - `batch_processor.py` → Calcola metriche OHLC, RSI, Bollinger
5. **Dashboard** → Visualizza dati real-time e batch

## 📈 Metriche Dashboard

- **Real-Time Trend**: Prezzi ultimi 12 ore (finestra mobile)
- **Daily Metrics**: OHLC, RSI, Bollinger Bands, Momentum
- **Memory Usage**: RAM dei container Docker
- **End-to-End Latency**: Latenza producer → dashboard per sensore

## 🧠 Anomaly Detection

Il sistema usa un modello statistico **3-sigma**:
- Calcola media e deviazione standard per ogni sensore
- Un dato è anomalo se: `|valore - media| > 3 * std_dev`
- Il modello viene ri-addestrato ogni 5 minuti

## 📁 Struttura Progetto

```
Lambda_Architecture/
├── docker-compose.yml       # Orchestrazione servizi
├── dashboard/               # Serving Layer (Flask)
│   ├── app.py
│   └── templates/index.html
├── iot-producer/            # Data Source
│   ├── kafka_producer.py
│   └── start.py
├── kafka-consumers/         # Batch + Speed Layer
│   ├── batch_layer_consumer.py
│   └── speed_layer_consumer.py
├── spark-jobs/              # Batch Processing
│   ├── batch_processor.py
│   └── model_trainer.py
├── spark-scheduler/         # Job Orchestrator
│   └── scheduler.py
└── cassandra-config/        # Speed Layer Config
    └── init.cql
```

## ⚙️ Configurazione

Le principali variabili sono configurate nel `docker-compose.yml`:

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `BATCH_SIZE` | 1000 | Messaggi per batch HDFS |
| `BATCH_FLUSH_INTERVAL` | 120s | Intervallo flush HDFS |
| `MODEL_TRAINER_INTERVAL` | 300s | Intervallo training modello |
| `BATCH_PROCESSOR_INTERVAL` | 150s | Intervallo elaborazione batch |

## 📝 License

MIT License