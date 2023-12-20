import importlib.metadata

__version__ = importlib.metadata.version("airflow-provider-radical")


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
        "versions": [__version__],
        "docs-url": "https://github.com/ejjordan/airflowHPC/blob/main/README.md",
    }
