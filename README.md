# airflowHPC

## Description
This repository contains the code for the airflowHPC project. 
The project is a proof of concept for using Apache Airflow to manage HPC jobs,
in particular complex molecular dynamics simulation workflows.

## Installation

### Clone the repository
```bash
git clone https://github.com/ejjordan/airflowHPC.git
```

### Install the package in a virtual environment
```bash
export AIRFLOW_HOME=$PWD/airflow_dir
python3 -m venv airflowHPC_env
source airflowHPC_env/bin/activate
pip install --upgrade pip
pip install -e airflowHPC/
```

### Install the package requirements
```bash
pip install -r airflowHPC/requirements.txt
```

It may be necessary to first install gmxapi manually as described in the
[gmxapi installation instructions](https://manual.gromacs.org/current/gmxapi/userguide/install.html).

```bash
gmxapi_ROOT=/path/to/gromacs pip install --no-cache-dir gmxapi
```

### Simplest demonstration
This demo show how to run grompp and mdrun in a simple Airflow DAG.
It should complete in less than a minute.

```bash
airflow users create --username admin --role Admin -f firstname -l lastname -e email@fake.org
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=airflowHPC/airflowHPC/dags/
export AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW="graph"
airflow standalone
```

This will start a webserver on port 8080 where you can trigger airflow DAGs.
The first command will prompt you to enter a password for the admin user.
You will then use this username and password to log in to the webserver.

On the webserver, navigate to the `DAGs` tab and click `run_gmxapi` DAG.
If you press the play button in the upper right corner, the DAG will begin running.

Note that since this is still very WIP, 
if you run the `run_gmxapi` DAG multiple times 
it will overwrite the output files from previous runs.
The output files currently live in a directory called `outputs`
in the directory where you ran the `airflow standalone` command.

### Setting up a database connection for more complex workflows
On the webserver of the simple demonstration, you may have noticed warnings
that you should not use the SequentialExecutor or SQLite database in production.
There are detailed instructions for setting up a database connection in the Airflow 
documentation 
[here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html).

#### PostgreSQL setup
It is possible to install PostgreSQL on HPC resources with spack.
```bash
spack install postgresql
```
Now to set up the database, you can use the following commands.
```bash
spack load postgresql
mkdir postgresql_db
initdb -D postgresql_db/data
pg_ctl -D postgresql_db/data -l logfile start
```
Note that you will need to run the `pg_ctl` command every time you want to use the database,
for example after logging out and back in to the HPC resource.

The airflow database can then be set up with the following commands.
```bash
createdb -T template1 airflow_db
psql airflow_db
```
In the psql shell, you can then run the following commands.
```sql
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL ON SCHEMA public TO airflow_user;
ALTER USER airflow_user SET search_path = public;
quit
```
You may also need to put the postgresql library in the `LD_LIBRARY_PATH`
environment variable.
```bash
export LD_LIBRARY_PATH=/path/to/your/postgresql/lib:$LD_LIBRARY_PATH
```

Once you have a database set up and configured according to the instructions,
you can install the airflowHPC postgresql requirements.
```bash
pip install airflowHPC[postgresql]
```

You can then launch the standalone webserver with the `prepare_standalone.sh` script.
This script will use the RadicalLocalExecutor, which is vended by the function
`get_provider_info()` in `airflowHPC/__init__.py`.
```bash
bash airflowHPC/scripts/prepare_standalone.sh
```

Note that the script can be run from any directory, but it should not be moved
from its original location in the repository.

### Running from the command line
After conviguring a python virtual environment and MySQL database as described above,
you can run a DAG using the
[Airflow CLI](https://airflow.apache.org/docs/apache-airflow/stable/howto/usage-cli.html).
Note that you must configure the airflow environment variables as described above so
that the CLI can find the DAGs and the database.
**You also need to have set up the airflow configuration by, for example, running the
`airflow standalone` or `airflow db init` command at least once.**

```bash
airflow dags backfill -s 2024-03-01 --reset-dagruns -y run_gmxapi
```
The script should work without needing to configure any environment variables,
as they are set in the script itself and the help should provide enough information
to run the script.

### Debugging
Instructions for debugging a DAG are available in the 
[airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/debug.html#testing-dags-with-dag-test).

It is also possible to debug an operator, task, or DAG, by setting the
environment variable `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT` to a large value
in seconds and dropping a set_trace() statement in the code.

### Clearing the database
Airflow ships with a command line tool for clearing the database.
```bash
airflow db clean --clean-before-timestamp 'yyyy-mm-dd'
```
The dag runs can also be deleted from the webserver.
