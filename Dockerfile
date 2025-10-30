FROM apache/airflow:2.9.2

USER root

# Copiar el archivo de requirements
COPY requirements.txt /requirements.txt

USER airflow

# Instalar las dependencias como usuario airflow
RUN pip install --no-cache-dir -r /requirements.txt