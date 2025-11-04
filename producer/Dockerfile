FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script
COPY kafka_producer.py .

# Command to run the script
ENTRYPOINT ["python", "kafka_producer.py"]
