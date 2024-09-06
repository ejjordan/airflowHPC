try:
    from importlib.metadata import version as importlib_metadata_version
except ImportError:
    from importlib_metadata import version as importlib_metadata_version

__version__ = importlib_metadata_version("apache-airflow-providers-hpc")


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-hpc",
        "name": "airflowHPC",
        "description": "AirflowHPC is a provider package for Airflow",
        "config": {
            "hpc": {
                "description": "Configure AirflowHPC provider",
                "options": {
                    "cores_per_node": {
                        "description": "Number of cores per node",
                        "type": "string",
                        "default": "32",
                        "example": "32",
                        "version_added": "0.0.0",
                        "sensitive": False,
                    },
                    "gpus_per_node": {
                        "description": "Number of GPUs per node",
                        "type": "string",
                        "default": "0",
                        "example": "0",
                        "version_added": "0.0.0",
                        "sensitive": False,
                    },
                    "gpu_type": {
                        "description": "Type of GPU. Can be set to 'rocm', 'hip', 'nvidia' or 'None'",
                        "type": "string",
                        "default": "None",
                        "example": "nvidia",
                        "version_added": "0.0.0",
                        "sensitive": False,
                    },
                    "mem_per_node": {
                        "description": "Memory per node in GB",
                        "type": "string",
                        "default": "64",
                        "example": "64",
                        "version_added": "0.0.0",
                        "sensitive": False,
                    },
                    "threads_per_core": {
                        "description": "Number of threads per core",
                        "type": "string",
                        "default": "2",
                        "example": "2",
                        "version_added": "0.0.0",
                        "sensitive": False,
                    },
                },
            },
        },
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
            "airflowHPC.executors.test_executor.TestExecutor",
            "airflowHPC.executors.resource_executor.ResourceExecutor",
        ],
        "versions": [__version__],
        "docs-url": "https://github.com/ejjordan/airflowHPC/blob/main/README.md",
    }
