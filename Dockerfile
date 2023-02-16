FROM apache/airflow:2.5.1

USER root

RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
ENV AIRFLOW_CONN_SPARK_MASTER spark://spark-master:7077?deploy-mode=client&spark_binary=spark-submit

RUN export JAVA_HOME

USER airflow

COPY requirements.txt /
RUN pip install -r /requirements.txt