FROM postgres:17

COPY init/airflow_init.sql /docker-entrypoint-initdb.d/01-airflow_init.sql
COPY init/fpl_init.sql /docker-entrypoint-initdb.d/02-fpl_init.sql
COPY init/fpl_data_dump.sql /docker-entrypoint-initdb.d/03-fpl_data_dump.sql