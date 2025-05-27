from airflow.decorators import task
from airflow.exceptions import AirflowException


@task
def iterations_completed(dataset_dict, max_iterations):
    import logging
    import os

    highest_completed_iteration = 0
    for iteration in range(1, max_iterations + 1):
        iteration_name = f"iteration_{iteration}"
        if iteration_name in dataset_dict:
            iteration_data = dataset_dict[iteration_name]
            logging.info(f"iteration_data: {iteration_data}")
            if "params" in iteration_data:
                num_swarms = iteration_data["params"]["num_string_points"]
                logging.info(f"num_swarms: {num_swarms}")
            else:
                logging.error(f"No params in {iteration_name}")
                raise AirflowException(f"No params in {iteration_name}")
            for swarm_idx in range(num_swarms):
                if str(swarm_idx) not in iteration_data:
                    logging.info(f"No data for swarm {swarm_idx}")
                    break
                iteration_data_point = iteration_data[str(swarm_idx)]
                logging.info(f"iteration_data_point: {iteration_data_point}")
                if "sims" in iteration_data_point:
                    sims = iteration_data_point["sims"]
                    logging.info(f"sims: {sims}")
                    for sim in sims:
                        if "gro" in sim:
                            if "simulation_id" not in sim:
                                logging.error(f"No simulation_id in {sim}.")
                                raise AirflowException(f"No simulation_id in {sim}.")
                            if not os.path.exists(sim["gro"]):
                                logging.error(f"Missing gro file: {sim['gro']}")
                                raise AirflowException(f"File {sim['gro']} not found.")
                            logging.info(
                                f"Iteration {iteration}, swarm {swarm_idx}, simulation {sim['simulation_id']} has gro file."
                            )
                        else:
                            logging.info(
                                f"No gro file in iteration {iteration}, simulation {sim['simulation_id']}"
                            )
                            break
                    highest_completed_iteration = iteration
                    logging.info(
                        f"Iteration {iteration}, swarm {swarm_idx} has all gro files."
                    )
                else:
                    logging.info(f"No sims in iteration {iteration}")
                    break
        else:
            logging.info(f"No data for iteration {iteration}")
            break

    return highest_completed_iteration
