#/Users/matteo/Documents/Lambda_IoT/iot-producer/Dockerfile

# Partiamo da un'immagine Python 3.9 ufficiale e "slim" (leggera)
FROM python:3.9-slim

# Impostiamo una directory di lavoro all'interno del container
WORKDIR /app

# Copiamo prima il file dei requisiti
COPY requirements.txt .

# Installiamo le dipendenze Python necessarie
RUN pip install --no-cache-dir -r requirements.txt

# Copiamo il resto del codice sorgente (start.py E producer.py)
COPY . .

# Comando da eseguire quando il container viene avviato
# CORREZIONE: Avvia lo script di inizializzazione
CMD ["python", "start.py"]