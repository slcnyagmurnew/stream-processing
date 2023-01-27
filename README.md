

For using **'postgres'** from *'airflow.providers'*:

```angular2html
pip install apache-airflow[postgres]
```

Uncomment
```angular2html
#    - ./logs:/opt/airflow/logs
#    - ./plugins:/opt/airflow/plugins
``` 
rows in docker-compose.yml to see logging from Kafka. Otherwise, it gives secret_key error not matching (Kafka can not find logs directory).

If custom airflow image does not exist, build it first with uncomment
```angular2html
#  build: .
```