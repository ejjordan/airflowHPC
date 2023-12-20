from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone


@task.radical
def print_source(**kwargs):
    import logging, inspect

    logging.info(
        f"Printing the task function:\n{inspect.getsource(kwargs['task'].python_callable)}"
    )


with DAG("get_source", start_date=timezone.utcnow(), catchup=False) as dag:
    print_source()

if __name__ == "__main__":
    dag.test()
