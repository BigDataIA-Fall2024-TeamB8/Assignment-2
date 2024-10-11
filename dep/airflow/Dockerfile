# Use the official Apache Airflow image with Python 3.8+
FROM apache/airflow:2.4.3-python3.8

# Copy requirements file and install any additional dependencies
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Set the Airflow user for running the application
USER airflow
