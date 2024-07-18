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
This demo shows how to run grompp and mdrun in a simple Airflow DAG.
It should complete in less than a minute.

```bash
airflow users create --username admin --role Admin -f firstname -l lastname -e your.email@mail.org
export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.resource_executor.ResourceExecutor
export AIRFLOW__CORE__DAGS_FOLDER=airflowHPC/airflowHPC/dags/
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW="graph"
airflow standalone
```

This will start a webserver on port 8080 where you can trigger airflow DAGs.
The first command will prompt you to enter a password for the admin user.
You will then use this username and password to log in to the webserver.

On the webserver, navigate to the `DAGs` tab and click `run_gmx` DAG.
If you press the play button in the upper right corner, the DAG will begin running.


The output files are written by default to a directory called `outputs`, though this
is a DAG parameter which can be changed in the browser before triggering the DAG.
Note that if you run the `run_gmx` DAG multiple times with the same
output directory, the files will be overwritten.
The output directory will be located in the directory where you ran
the `airflow standalone` command.

### Setting up a database connection for more complex workflows
On the webserver of the simple demonstration, you may have noticed warnings
that you should not use the SequentialExecutor or SQLite database in production.
There are detailed instructions for setting up a database connection in the Airflow 
documentation 
[here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html).

#### MySQL setup (suitable for local execution)
MySQL is available from package managers such as apt or on Ubuntu.
```bash
sudo apt install mysql-server
```
You can then set up the database with the following commands.

```bash
sudo systemctl start mysql
sudo mysql --user=root --password=root -e "CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
sudo mysql --user=root --password=root -e "CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';"
sudo mysql --user=root --password=root -e "GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user'"
sudo mysql --user=root --password=root -e "FLUSH PRIVILEGES;"
```

If you don't have root access, you can use the postgresql instructions below,
which do not require root access.

#### PostgreSQL setup (suitable for HPC resources)
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

This script will use the ResourceExecutor, which is vended by the function
`get_provider_info()` in `airflowHPC/__init__.py`.
```bash
bash airflowHPC/scripts/prepare_standalone.sh
```

Note that the script can be run from any directory, but it should not be moved
from its original location in the repository.
The script should work without needing to configure any environment variables,
as they are set in the script itself and the help should provide enough information
to run the script.

### Running from the command line

After conviguring a python virtual environment and database as described above,
you can run a DAG using the
[Airflow CLI](https://airflow.apache.org/docs/apache-airflow/stable/howto/usage-cli.html).
Note that you must configure the airflow environment variables as described above so
that the CLI can find the DAGs and the database.
**You also need to have set up the airflow configuration by, for example, running the
`airflow standalone` or `airflow db init` command at least once.**

```bash
airflow dags backfill -s YYYY-MM-DD --reset-dagruns -y run_gmxapi
```

### Running on an HPC resources

Below is an example slurm script for running a DAG on an HPC resource.
```bash
#!/bin/bash -l

#SBATCH --account=your_account
#SBATCH --job-name=airflow
#SBATCH --partition=gpu
#SBATCH --time=01:00:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=32
#SBATCH --cpus-per-task=4

export SRUN_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK
export OMP_PLACES=cores

source /path/to/spack/share/spack/setup-env.sh
spack load postgresql@15.2
source /path/to/pyenvs/spack_py/bin/activate
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db"
export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.resource_executor.ResourceExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="/path/to/airflowHPC/airflowHPC/dags/"
export AIRFLOW__HPC__CORES_PER_NODE=128
export AIRFLOW__HPC__GPUS_PER_NODE=8
export AIRFLOW__HPC__GPU_TYPE="nvidia"
export AIRFLOW__HPC__MEM_PER_NODE=256
export AIRFLOW__HPC__THREADS_PER_CORE=2
export RADICAL_UTILS_NO_ATFORK=1
export LD_LIBRARY_PATH=/path/to/postgresql/lib/:$LD_LIBRARY_PATH
pg_ctl -D /path/to/databases/postgresql/data/ -l /path/to/databases/postgresql/server.log start

module load gromacs

airflow dags backfill -s 2024-06-14 -v --reset-dagruns -y gmx_multi
```

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
