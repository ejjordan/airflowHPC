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
pip install -r airflowHPC/requirements.txt
```

This will also install all the necessary dependencies.

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
```bash
standalone | Login with username: admin  password: ****************
```
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
```bash
bash airflowHPC/scripts/prepare_standalone.sh
```

Note that the script can be run from any directory, but it should not be moved
from its original location in the repository.