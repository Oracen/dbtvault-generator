import subprocess
from typing import List

from dbtvault_generator.constants import types


def run_shell_operation(command: List[str]) -> types.PipeOutput:
    """This is ugly. Hurry up 1.5"""
    pipe = subprocess.run(command, capture_output=True, text=True)
    run_ok = pipe.returncode == 0
    return (pipe.stdout, pipe.stderr, run_ok)
