import subprocess
from typing import List

from dbtvault_generator.constants import exceptions


def run_shell_operation(command: List[str]) -> str:
    """This is ugly. Hurry up 1.5"""
    pipe = subprocess.run(command, capture_output=True, text=True)
    output = pipe.stdout
    if not pipe.returncode != 0:
        output = pipe.stdout.replace("\n", "")
        raise exceptions.SubprocessFailed(
            f"Call to subprocess did not exit with code 0, text printed: {output}"
        )

    return output
