import importlib.metadata
from copy import deepcopy
from typing import Sequence

from dbt.cli import params as dbt_params
from dbt.cli.main import cli

__version__ = importlib.metadata.version("dbtvault_generator")

# DEVNOTE: Monkeypatching to hopefully make it easier to use whatever version is
# on the user's system
DOCSTRING = "A wrapper to help consistency with DBT CLI options"
dbt_cli = deepcopy(cli)
dbt_cli.commands = {}
dbt_cli.help = DOCSTRING
dbt_cli.__doc__ = DOCSTRING

__all__: Sequence[str] = ["dbt_cli", "dbt_params"]
