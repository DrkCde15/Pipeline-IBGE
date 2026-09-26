# Imagem do Airflow + Java (PySpark) + deps do projeto.
# A imagem oficial não traz JDK nem pyspark, e as tasks de transformação precisam deles.
# Airflow 2.11.2 (última série 2.x, EOL abr/2026) em Python 3.12 para acompanhar o ambiente local.
FROM apache/airflow:2.11.2-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    pandas \
    pyarrow \
    sqlalchemy \
    psycopg2-binary \
    python-dotenv \
    requests \
    "pandera>=0.20"
