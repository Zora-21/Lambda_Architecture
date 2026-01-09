#/Users/matteo/Documents/Lambda_IoT/dashboard/Dockerfile
# Partiamo da un'immagine Python 3.9 ufficiale e "slim" (leggera)
FROM python:3.9-slim

# Impostiamo una directory di lavoro all'interno del container
WORKDIR /app

# Copiamo prima il file dei requisiti per installare le dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamo tutto il resto del codice del dashboard (app.py, cartella templates)
COPY . .

# Esponiamo la porta 5000 (porta di default di Flask) all'interno del container
# Il file docker-compose.yml collegherà questa porta alla porta 5000 della macchina host
EXPOSE 5000

# Comando per avviare l'applicazione Flask
# 'flask run' avvia il server
# '--host=0.0.0.0' lo rende accessibile dall'esterno del container (fondamentale)
CMD ["flask", "run", "--host=0.0.0.0"]