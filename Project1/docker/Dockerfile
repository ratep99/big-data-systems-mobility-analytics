FROM bde2020/spark-python-template:3.1.2-hadoop3.2

COPY denverMobility.py /app/

ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/denverMobility.py
ENV SPARK_APPLICATION_ARGS "2023-02-11 09:00:00 2023-02-11 10:00:00"

