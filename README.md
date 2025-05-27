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

## Install the package 
### new virtual environment

It is generally a good idea to create a new virtual environment for each project.
It is not required to install an editable version of the package,
but it will make it easier to get the latest fixes and features.

```bash
export AIRFLOW_HOME=$PWD/airflow_dir
python3 -m venv airflowHPC_env
source airflowHPC_env/bin/activate
pip install --upgrade pip
pip install -e airflowHPC/
```

### existing virtual environment

If you already have an existing virtual environment you would like to use,
you can just source that environment and install the package.
It is not required to install an editable version of the package,
but it will make it easier to get the latest fixes and features.

```bash
source path/to/venv/bin/activate
pip install -e airflowHPC/
```

## Setting up a database connection

There are detailed instructions for setting up a database connection in the Airflow 
documentation 
[here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html),
but the instructions below might be a little easier to follow.

### MySQL setup (suitable for local execution)
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

### PostgreSQL setup (suitable for HPC resources)
It is possible to install PostgreSQL on HPC resources with spack.
You can follow the commands below or simply run the `prepare_spack.sh` script in the
`scripts` directory of the repository.

##### Manual spack installation
If you don't already have a spack installation, you can simply clone the repo.

```bash
git clone -c feature.manyFiles=true --depth=2 https://github.com/spack/spack.git
```
You can then install postgresql with the following command.
```bash
spack install postgresql
```
##### Setting up the postgres database
The database can be set up manually with the commnads that follow,
or you can run the `postgres_setup.sh` script in the `scripts` 
directory of the repository.

The `max_connections` flag is important for being able to run large number of tasks
simultaneously, as it sets the maximum number of tasks.
The `shared_buffers` flag ensures that the database has enough memory to run large
numbers of tasks simultaneously.
If you need to run on a port that is not the default (5432), for example because someone
else is using that port, you can set the `port` flag to the desired port number
by adding `--set port=myportnum` to the `initdb` command.
If you don't have write permissions to `/tmp` or don't want you database PIDs to be
visible on a shared system, you can use the `--set unix_socket_directories` flag to
set the directory where the database PIDs will be stored.

```bash
spack load postgresql
mkdir postgresql_db
initdb -D postgresql_db/data --set listen_addresses='*' --set max_connections=512 --set shared_buffers="1024MB"
pg_ctl -D postgresql_db/data -l postgresql_db/logfile start
```

The airflow database can then be set up with the following commands.
```bash
createdb -T template1 airflow_db
psql airflow_db -f airflowHPC/scripts/airflow_db.sql
```

### Finish the package installation

Once you have a database set up and configured according to the instructions,
you can install the airflowHPC postgresql requirements.
```bash
pip install airflowHPC[postgresql]
```

### Install the package requirements

There are a few python packages that are not required for installation,
but are required for running the example DAGs.

```bash
pip install -r airflowHPC/requirements.txt
```

### Simple DAG example
This demo shows how to run grompp and mdrun in a simple Airflow DAG.
It should complete in less than a minute.

```bash
airflow users create --username admin --role Admin -f firstname -l lastname -e your.email@mail.org
airflow users reset-password --username admin --password admin
export AIRFLOW__CORE__EXECUTOR=airflowHPC.executors.resource_executor.ResourceExecutor
export AIRFLOW__CORE__DAGS_FOLDER=airflowHPC/airflowHPC/dags/
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW="graph"
export AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE=False
export AIRFLOW__HPC__CORES_PER_NODE=16
export AIRFLOW_HOME=$PWD/airflow
export RADICAL_UTILS_NO_ATFORK=1
airflow standalone
```

This will start a webserver on port 8080 where you can trigger airflow DAGs.
If you need to run the webserver on a different port, you can set the
AIRFLOW__WEBSERVER__WEB_SERVER_PORT environment variable.
The first command will prompt you to enter a password for the admin user.
You will then use this username and password to log in to the webserver.

On the webserver, navigate to the `DAGs` tab and click `run_gmx` DAG.
If you press the play button in the upper right corner, you can specify the
output directory (or use the default value) and trigger the DAG.


The output files are written by default to a directory called `run_gmx`, though this
is a DAG parameter which can be changed in the browser before triggering the DAG.
Note that if you run the `run_gmx` DAG multiple times with the same
output directory, the files will be overwritten.
The output directory will be located in the directory where you ran
the `airflow standalone` command.


### Running from the command line

After configuring a python virtual environment and database as described above,
you can run a DAG using the
[Airflow CLI](https://airflow.apache.org/docs/apache-airflow/stable/howto/usage-cli.html).
Note that you must configure the airflow environment variables as described above so
that the CLI can find the DAGs and the database.
**You also need to have set up the airflow configuration by, for example, running the
`airflow standalone` or `airflow db init` command at least once.**

```bash
airflow dags backfill -s YYYY-MM-DD --reset-dagruns -y run_gmx
```

It is also possible to trigger a dag from the command line while running the 
airflow scheduler (possibly in a different terminal).
```bash
airflow dags trigger run_gmx
```
```bash
airflow scheduler
```
This is especially useful for interactive sessions on an HPC resource,
whereas the backfill command is more useful for batch jobs, since it does not
need a scheduler running. Note that the option specified above,
`AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE=False`, is set so that the scheduler
does not run jobs automatically. This means it is also possible to set 
the environment variable `export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False`,
which will unpause all the DAGs. Otherwise it is necessary to unpause the DAGs
with the webserver or the CLI using the command
```bash
airflow dags unpause run_gmx
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

### extra stuff
Note that you will need to run the `pg_ctl` command every time you want to use the database,
for example after logging out and back in to the HPC resource.