from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone


@task
def log_template(template):
    import logging
    logging.info(f"Template: {template}")
    return template


@task
def unpack_ref_t(**context):
    import logging
    tl = context["task"].render_template("{{ params.ref_t_list | list}}", context)
    logging.info(f"Unpacked ref_t_list: {tl}")
    expanded = [{"ref_t": ref_t} for ref_t in tl]
    logging.info(f"Expanded ref_t_list: {expanded}")
    return expanded

with DAG(
    "template_log",
    start_date=timezone.utcnow(),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={"ref_t_list": [300, 310, 320, 330]},
) as template_log:
    t_list = unpack_ref_t()
    #log = log_template.expand(template=t_list)
    #log_template(template=log.get_task_map_length())
    #import ipdb;ipdb.set_trace()
