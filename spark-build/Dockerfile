FROM bitnami/spark:3.3
USER root

COPY requirements.txt /
RUN pip install -r /requirements.txt
# Installing package into Spark if needed

EXPOSE 8080
EXPOSE 9090
EXPOSE 7077