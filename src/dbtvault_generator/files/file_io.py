import json
from io import TextIOWrapper
from pathlib import Path
from typing import Dict, Type, Union

import yaml
from dbt_artifacts_parser import parser as dbta_parser  # type: ignore
from yaml.parser import ParserError

from dbtvault_generator.constants import exceptions, literals, types


class Loader(yaml.SafeLoader):
    """
    Full credit to: https://stackoverflow.com/questions/
        528281/how-can-i-include-a-yaml-file-inside-another
    """

    def __init__(self, stream: TextIOWrapper):
        self._root = Path(stream.name).parent

        super(Loader, self).__init__(stream)

    def include(self, node: yaml.ScalarNode) -> types.Mapping:
        filename = self._root / str(self.construct_scalar(node))
        with open(filename, "r") as stream:
            data: types.Mapping = yaml.load(stream, Loader)
            data.pop("version", None)
            return data


Loader.add_constructor("!include", Loader.include)


def read_yml_file(
    filepath: Union[Path, str], excepion: Type[Exception], message: str
) -> types.Mapping:
    try:
        with open(filepath, "r") as stream:
            output: types.Mapping = yaml.load(stream, Loader)
    except ParserError:
        raise excepion(message)
    return output


def write_yaml_file(
    filepath: Union[Path, str],
    data: types.Mapping,
) -> None:
    with open(filepath, "w") as stream:
        yaml.dump(data, stream, sort_keys=False)


def write_text(filepath: Union[Path, str], payload: str):
    with open(filepath, "w") as buffer:
        buffer.write(payload)


"""
DEVNOTE:

Parsing the artifacts here is kind of an exception to keeping the loading here and
the param parsing elsewhere. The dbt-artifact-parser library has so many untyped or
wildly typed parameters (union all the things!) that I think it will be better to
isolate the bits I want to use here, and return a nice clean subset of the total
object.
"""


def load_catalog(target_path: Path) -> types.DbtCatalog:
    catalog_path = target_path / literals.DEFAULT_NAME_CATALOG
    if not catalog_path.is_file():
        raise exceptions.DbtArtifactError(
            f"Catalog file {literals.DEFAULT_NAME_CATALOG} does not exist. Run "
            "`docs generate` to create file"
        )
    try:
        with open(catalog_path, "r") as stream:
            data = json.load(stream)
    except json.JSONDecodeError:
        raise exceptions.DbtArtifactError(
            f"Catalog file {literals.DEFAULT_NAME_CATALOG} could not be loaded due "
            "to parsing error. Rebuild it as it likely contains errors"
        )
    catalog = dbta_parser.parse_catalog(data)  # type: ignore
    models: Dict[str, types.CatalogModel] = {}
    for key, value in catalog.nodes.items():
        if not key.startswith("model"):
            continue
        columns: Dict[str, types.CatalogModelColumn] = {
            name: types.CatalogModelColumn(name=name, dtype=metadata.type)
            for name, metadata in value.columns.items()
        }
        model = types.CatalogModel(name=value.metadata.name, columns=columns)
        models[model.name] = model
    return types.DbtCatalog(models=models)
