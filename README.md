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
This demo show how a replica exchange molecular dynamics simulation might look as 
an airflow DAG. 
In the default configuration, the DAG will run a four replicas for two iterations. 
It should complete in less than a minute.

```bash
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=airflowHPC/airflowHPC/dags/
airflow standalone
```

This will start a webserver on port 8080 where you can trigger airflow DGAs.
Several lines of startup output will be printed to the terminal, including a
line that looks like this:

`standalone | Login with username: admin  password: ****************`


where the password is a randomly generated string. 
You can use these credentials to log in to the webserver.

On the webserver, navigate to the `DAGs` tab and click `looper` DAG.
If you press the play button in the upper right corner, the DAG will begin running.
You can monitor the progress of the DAG by clicking on the `Graph View` tab.

Note that since this is still very WIP, 
if you run the `looper` DAG multiple times 
it will overwrite the output files from previous runs.
The output files currently live in a directory called `outputs`
in the directory where you ran the `airflow standalone` command.

### More complex demonstration
On the webserver of the simple demonstration, you may have noticed warnings
that you should not use the SequentialExecutor or SQLite database in production.
There are detailed instructions for setting up a MySQL database in the Airflow 
documentation 
[here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html).

Once you have a MySQL database set up and configured according to the instructions,
you can launch the standalone webserver with the `prepare_standalone.sh` script.
This script will use the RadicalLocalExecutor, which is vended by the function
`get_provider_info()` in `airflowHPC/__init__.py`.
```bash
bash airflowHPC/scripts/prepare_standalone.sh
```

Note that the script can be run from any directory, but it should not be moved
from its original location in the repository.

### Running from the command line
After conviguring a python virtual environment and MySQL database as described above,
you can run a DAG from the command line by executing the `base_runner.py` script.
```bash
python airflowHPC/scripts/base_runner.py get_mass
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
