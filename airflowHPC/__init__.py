try:
    from importlib.metadata import version as importlib_metadata_version
except ImportError:
    from importlib_metadata import version as importlib_metadata_version

__version__ = importlib_metadata_version("apache-airflow-providers-hpc")


def get_provider_info():
    return {
        "package-name": "airflow-provider-radical",
        "name": "AirflowHPC",
        "description": "AirflowHPC is a provider package for Airflow",
        "operators": [
            {
                "module": "airflowHPC.operators.radical_operator.RadicalOperator",
            }
        ],
        "task-decorators": [
            {
                "name": "radical",
                "class-name": "airflowHPC.operators.radical_operator.radical_task",
            }
        ],
        "executors": [
            "airflowHPC.executors.radical_local_executor.RadicalLocalExecutor",
            "airflowHPC.executors.radical_local_executor.RadicalExecutor",
            "airflowHPC.executors.zmq_sequential_executor.ZMQSequentialExecutor",
        ],
        "versions": [__version__],
        "docs-url": "https://github.com/ejjordan/airflowHPC/blob/main/README.md",
    }
