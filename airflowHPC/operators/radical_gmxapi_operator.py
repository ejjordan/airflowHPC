import os
from airflow.operators.bash import BashOperator
from typing import Container, Sequence
from gmxapi.commandline import cli_executable
from typing import Iterable, Union


class RadicalGmxapiOperator(BashOperator):
    template_fields: Sequence[str] = ("bash_command", "env")
    template_fields_renderers = {"bash_command": "bash", "env": "json"}

    def __init__(
        self,
        executable: str,
        gmx_args: list,
        input_files: dict,
        output_files: dict,
        output_dir: str,
        stdin=None,
        env: dict[str, str] | None = None,
        append_env: bool = False,
        output_encoding: str = "utf-8",
        skip_on_exit_code: int | Container[int] | None = 99,
        **kwargs,
    ) -> None:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        out_dir_full_path = os.path.abspath(output_dir)
        output_files_paths = {
            f"{k}": f"{os.path.join(out_dir_full_path, v)}"
            for k, v in output_files.items()
        }
        bash_command = self.create_call(
            executable,
            gmx_args,
            input_files,
            output_files_paths,
        )
        super().__init__(
            bash_command=bash_command,
            cwd=out_dir_full_path,
            env=env,
            append_env=append_env,
            output_encoding=output_encoding,
            skip_on_exit_code=skip_on_exit_code,
            **kwargs,
        )

    def flatten_dict(self, mapping: dict):
        for key, value in mapping.items():
            yield str(key)
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                yield from [str(element) for element in value]
            else:
                yield value

    def create_call(
        self,
        executable=None,
        arguments=(),
        input_files: Union[dict, Iterable[dict]] = None,
        output_files: Union[dict, Iterable[dict]] = None,
    ):
        if executable is None:
            executable = cli_executable()
        if input_files is None:
            input_files = {}
        if output_files is None:
            output_files = {}
        try:
            executable = str(executable)
        except Exception as e:
            raise TypeError(
                "This operator requires paths and names to be strings. *executable* argument is "
                f"{type(executable)}."
            )
        if isinstance(arguments, (str, bytes)):
            arguments = [arguments]

        call = list()
        call.append(executable)
        call.extend(arguments)
        call.extend(self.flatten_dict(input_files))
        call.extend(self.flatten_dict(output_files))
        return call


"""
myargs={'executable': cli_executable(),
        'args': ['grompp'],
        'input_files': {'-f': '/tmp/tmp47zi9quc.mdp',
                        '-c': '/home/joe/experiments/airflow-hpc/executor/fs_peptide/npt_equil/step_0/sim_2/npt.gro',
                        '-r': '/home/joe/experiments/airflow-hpc/executor/fs_peptide/npt_equil/step_0/sim_2/npt.gro',
                        '-p': '/home/joe/experiments/airflow-hpc/executor/fs_peptide/prep/topol.top',
                        '-t': '/home/joe/experiments/airflow-hpc/executor/fs_peptide/npt_equil/step_0/sim_2/npt.cpt'},
        'output_files': {'-o': 'sim.tpr'},}
"""
