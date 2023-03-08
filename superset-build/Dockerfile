FROM apache/superset
# Switching to root to install the required packages
USER root
# Example: installing the MySQL driver to connect to the metadata database
# if you prefer Postgres, you may want to use `psycopg2-binary` instead
RUN pip install psycopg2-binary
# Example: installing a driver to connect to Redshift
# Find which driver you need based on the analytics database
# you want to connect to here:
# https://superset.apache.org/installation.html#database-dependencies
# Switching back to using the `superset` user
USER superset

RUN superset superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

RUN superset superset db upgrade
RUN superset superset load_examples
RUN superset superset init
