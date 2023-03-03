import subprocess
from typing import List

from dbtvault_generator.constants import types


def run_dbtvault_operation(
    macro_name: str, cli_passthrough: List[str]
) -> types.PipeOutput:
    """This is ugly. Hurry up 1.5"""
    command = ["dbt", "run-operation"] + cli_passthrough
    pipe = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = pipe.communicate()
    run_ok = pipe.returncode == 0
    return (out.decode(), err.decode(), run_ok)
