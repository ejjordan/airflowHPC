from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone


@task
def print_source(**kwargs):
    import logging, inspect

    logging.info(
        f"Printing the task function:\n{inspect.getsource(kwargs['task'].python_callable)}"
    )


with DAG("rct_get_source", start_date=timezone.utcnow(), catchup=False) as dag:
    dag.doc = (
        "Demo of how to get source code of a task function within the task itself."
    )
    print_source()

if __name__ == "__main__":
    dag.test()
