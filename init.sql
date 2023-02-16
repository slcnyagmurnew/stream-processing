\c airflow;
CREATE TABLE logs(
                index SERIAL PRIMARY KEY,
                ip varchar(20),
                ds bigint not null,
                unq_dst_ip integer);
GRANT ALL PRIVILEGES ON TABLE logs TO airflow;