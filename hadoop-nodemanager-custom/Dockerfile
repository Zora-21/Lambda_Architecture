# 1. Immagine di base
FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

# 2. Passa a root
USER root

# 3. --- CORREZIONE: Point to Debian 9 (Stretch) Archive ---
RUN echo "deb http://archive.debian.org/debian/ stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security/ stretch/updates main" >> /etc/apt/sources.list

# 4. Installa cron
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# 5. Configura il Cron Job
COPY crontab.txt /etc/cron.d/hadoop-cron

# 6. Imposta i permessi corretti per il file crontab
RUN chmod 0644 /etc/cron.d/hadoop-cron

# 7. Applica il crontab per l'utente root
RUN crontab /etc/cron.d/hadoop-cron

# 8. Crea un file di log vuoto
RUN touch /var/log/cron.log
RUN chmod 0644 /var/log/cron.log

# 9. --- MODIFICA: Copia lo script in una cartella sicura ---
COPY start-services.sh /usr/local/bin/start-services.sh
RUN chmod +x /usr/local/bin/start-services.sh

# 10. --- MODIFICA: Usa il nuovo percorso per il CMD ---
CMD ["/usr/local/bin/start-services.sh"]