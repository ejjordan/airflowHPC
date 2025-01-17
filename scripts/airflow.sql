CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL ON SCHEMA public TO airflow_user;
ALTER USER airflow_user SET search_path = public;