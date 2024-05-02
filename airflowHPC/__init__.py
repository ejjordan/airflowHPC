import importlib.metadata

__version__ = importlib.metadata.version("apache-airflow-provider-radical")


def get_provider_info():
    return {
        "package-name": "apache-airflow-provider-radical",
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
            "airflowHPC.executors.test_executor.TestExecutor",
            "airflowHPC.executors.resource_executor.ResourceExecutor",
        ],
        "versions": [__version__],
        "docs-url": "https://github.com/ejjordan/airflowHPC/blob/main/README.md",
    }
