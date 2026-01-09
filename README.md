lambda-iot-project/
│
├── .gitignore                    # File per escludere file (es. __pycache__, .env)
├── .env                          # File per le variabili d'ambiente (porte, password, ecc.)
├── docker-compose.yml            # Il file principale per orchestrare tutti i servizi
├── README.md                     # Istruzioni su come avviare e usare il progetto
│
├── dashboard/                    # Servizio "Serving Layer"
│   ├── Dockerfile                # Istruzioni per costruire l'immagine del dashboard
│   ├── app.py                    # Applicazione web Python/Flask
│   ├── requirements.txt          # Dipendenze (es. flask, cassandra-driver)
│   └── templates/
│       └── index.html            # Pagina HTML per la dashboard
│
├── hadoop-job/                   # Codice per il Batch Layer (MapReduce - Legacy/Optional)
│   ├── mapper.py                 # Funzione Map
│   ├── reducer.py                # Funzione Reduce
│   └── run_job.sh                # Script (opzionale) per avviare il job su YARN
│
├── hadoop-nodemanager-custom/    # Configurazione custom per Hadoop NodeManager
│   ├── Dockerfile                # Dockerfile per il NodeManager personalizzato
│   ├── crontab.txt               # Configurazione cron
│   └── start-services.sh         # Script di avvio servizi
│
├── iot-producer/                 # Servizio "IoT Producer"
│   ├── Dockerfile                # Istruzioni per costruire l'immagine del producer
│   ├── producer.py               # Script che genera dati (per Kafka/HDFS/Cassandra)
│   └── requirements.txt          # Dipendenze
│
├── kafka-consumers/              # Consumatori Kafka per Batch e Speed Layer
│   ├── Dockerfile                # Dockerfile per i consumer
│   ├── batch_layer_consumer.py   # Consumer per il Batch Layer
│   ├── speed_layer_consumer.py   # Consumer per lo Speed Layer
│   └── requirements.txt          # Dipendenze
│
├── spark-jobs/                   # Job Spark per analisi e machine learning
│   ├── batch_processor.py        # Elaborazione batch dei dati
│   └── model_trainer.py          # Training del modello ML
│
├── spark-scheduler/              # Scheduler per i job Spark
│   ├── Dockerfile                # Dockerfile per lo scheduler
│   └── scheduler.py              # Script di scheduling
│
└── cassandra-config/             # Configurazione per lo "Speed Layer" e storage dati
    └── init.cql                  # Script per creare keyspace e tabelle