#!/bin/bash

# 1. Avvia il demone cron
# Non usiamo exec qui, lo lasciamo partire come servizio
echo "Avvio del demone cron..."
service cron start || /usr/sbin/cron

# 2. Verifica che il file di log esista per il tail
touch /var/log/cron.log

# 3. Avvia il tail del log in background (per il debug)
echo "Avvio streaming log di cron..."
tail -f /var/log/cron.log &

# 4. Avvia il NodeManager SENZA 'exec' immediato
# Questo permette allo script di monitorare entrambi, ma per semplicità in Docker
# lanciamo il NodeManager come processo finale bloccante, ma senza uccidere cron.
echo "Avvio del servizio YARN NodeManager..."

# Lanciamo cron e assicuriamoci che resti su
# Se 'exec' uccideva i processi figli, usiamo questo approccio standard per Docker multi-processo:
/usr/sbin/cron &

# Ora avvia NodeManager. Usiamo exec per passare i segnali di stop a lui,
# ma avendo lanciato cron in background SENZA legami stretti, dovrebbe sopravvivere
# o essere gestito dal processo padre.
exec yarn nodemanager