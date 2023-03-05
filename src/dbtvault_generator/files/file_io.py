from pathlib import Path
from typing import Type, Union

import yaml
from yaml.parser import ParserError

from dbtvault_generator.constants import types


def read_yml_file(
    filepath: Union[Path, str], excepion: Type[Exception], message: str
) -> types.Mapping:
    try:
        with open(filepath, "r") as stream:
            output: types.Mapping = yaml.safe_load(stream)
    except ParserError:
        raise excepion(message)
    return output


def write_text(filepath: Union[Path, str], payload: str):
    with open(filepath, "w") as buffer:
        buffer.write(payload)
