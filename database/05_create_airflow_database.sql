CREATE DATABASE airflow OWNER market_user;

\connect airflow

GRANT ALL PRIVILEGES ON SCHEMA public TO market_user;
