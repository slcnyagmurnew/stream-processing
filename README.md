## Stream Processing with Spark-Streaming, Kafka, Redis and Airflow

#### In this study, data is simulated as near-real streaming data.

### Workflow

![MarineGEO circle logo](schema/workflow_schema.png "MarineGEO logo")


- **Data Generation**: Data is generated as anonymized data. There are generated source ips that have features merged from real data.
Each data takes place in a single JSON file. These files are sent to relevant tools via Airflow DAG.
After sending, next data file name is written to **config.json** file.

  <br>
    Example file: <br>
  &nbsp;{ <br>
  &nbsp; "src_ip": "187.44.158.33", <br>  
  &nbsp; "unq_dst_ip": "3" <br>
  &nbsp; _other features_ <br>
  &nbsp; "timestamp": "2022-01-03 09:00:00" <br>
  &nbsp;} <br>
  

- **Data Distribution and Processing**: There are two places for data distribution: **Kafka** and **Redis**. <br><br>
  - **Kafka**: Each data has **different partitions** in one Kafka topic. There is ip-partition map was done before running application.
  Kafka data is used by **Spark Streaming**. Spark master and worker containers always listen the **LogTopic** and collect data with complete mode so
  data is never lost. Then, Spark uses another Kafka topic called **SparkStreamingTopic** to write average of _unq_dst_ip_ feature 
  for collected data of each ip, and also they are in different partitions in the topic. <br> <br>

  - **Redis Time Series Database (RTSDB)**: Each ip based data is sent to related key of database, they are separated. The data is used for time series forecasting 
  with **Facebook Prophet** model. Thanks to Prophet's work with **Spark** parallelization and grouping, each data has its own forecasting model.
  After training operation finished, RTSDB is cleaned to prevent overloading of cache mechanism. Training is scheduled with **Airflow**
  and historical data (which is cleaned from RTSDB) is dumped to **PostgreSQL Database** for consistency of data. 


- **Data Visualization**: Not yet


#### Notes for me:
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