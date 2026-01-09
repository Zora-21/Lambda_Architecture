FROM apache/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install requests

# Copy scheduler
COPY scheduler.py /opt/spark/scheduler.py

# Set working directory
WORKDIR /opt/spark

# Run scheduler
CMD ["python3", "/opt/spark/scheduler.py"]
