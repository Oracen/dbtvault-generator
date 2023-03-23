import abc
import warnings
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, get_args

import pydantic
import yaml
from yaml.parser import ParserError

from dbtvault_generator.constants import exceptions, literals, types
from dbtvault_generator.files import file_io


def recursive_merge(base: types.Mapping, updated: types.Mapping) -> types.Mapping:
    """Recursively merges 2 dicts, one on top of the other"""
    base = deepcopy(base)  # Base is really our "default config"
    updated = deepcopy(updated)
    keys: Set[str] = set(base)
    new_keys = set(updated)
    for key in keys:
        val: Any = base[key]
        if key in updated:
            if type(val) == dict:
                if type(updated[key] == dict):
                    base[key] = recursive_merge(base[key], updated[key])
            elif type(val) == list:
                # Handle lists by just appending any non-duplicate items
                base[key] += [item for item in updated[key] if key not in val]
            else:
                base[key] = updated[key]

    # Only iterate over the keys we have to
    outstanding: Set[str] = new_keys - keys
    for key in outstanding:
        base[key] = updated[key]

    return base


def convert_to_string(obj: Any) -> str:
    if isinstance(obj, Path):
        return obj.as_posix()
    return str(obj)


def reformat_arg_key(string: str) -> str:
    string_kebab = string.replace("_", "-")
    return f"--{string_kebab}"


def cli_passthrough_arg_parser(
    project_dir: Path, target_dir: Optional[str] = None
) -> List[str]:
    args_list: List[str] = ["--project-dir", str(project_dir.absolute())]
    if target_dir is not None:
        args_list += ["--target-path", target_dir]

    return args_list


def arg_handler(arg_string: Optional[str]) -> Dict[Any, Any]:
    if arg_string is None:
        return {}
    try:
        args: types.Mapping = yaml.safe_load(arg_string)
    except ParserError:
        raise exceptions.ArgParseError(
            "The arguments passed in the --args field is not a valid yml string"
        )
    return args


def get_dbt_project_config(
    project_path: Path, cli_target_dir: Optional[str]
) -> types.ProjectConfig:
    """Reads in the dbt_project file and pulls out required fields"""
    project_yml = project_path / "dbt_project.yml"
    if not (project_yml).is_file():
        raise exceptions.ProjectNotConfiguredError(
            "The specified directory is not a valid DBT project"
        )

    dbt_project = file_io.read_yml_file(
        project_yml,
        exceptions.ProjectNotConfiguredError,
        "The file dbt_project.yml is corrupted and cannot be read, "
        "please fix before continuing",
    )

    model_dirs: List[str] = dbt_project.get("model-paths", [])
    # Backwards compatibility
    model_dirs += dbt_project.get("source-paths", [])
    if len(model_dirs) == 0:
        raise exceptions.ProjectNotConfiguredError(
            "This project contains no model directories, code cannot be generated"
        )

    if cli_target_dir is None:
        target_dir: str = dbt_project.get("target-path", literals.DEFAULT_TARGET)
    else:
        target_dir = cli_target_dir

    return types.ProjectConfig(model_dirs=model_dirs, target_dir=target_dir)


def build_model_config(
    defaults: types.Mapping,
    new_config: types.Mapping,
    config_path: str,
) -> types.Mapping:
    defaults = deepcopy(defaults)
    if new_config.get(literals.DBTVG_TARGET_PATH_KEY, "") == "":
        # If emtpy, we get rid of overwrite and let base key be default
        defaults.pop(literals.DBTVG_TARGET_PATH_KEY, None)
    # Merge cleaned config
    new_config = recursive_merge(defaults, new_config)

    if new_config.get(literals.DBTVG_TARGET_PATH_KEY, "") == "":
        # If there's still no valid path, we add one based on the config loc
        new_config[literals.DBTVG_TARGET_PATH_KEY] = config_path
    return new_config


def model_param_factory(
    model_type: types.DBTVaultModel, params: types.Mapping
) -> types.DBTVGBaseModelParams:
    if model_type == "stage":
        return types.ModelStageParams(**params)
    elif model_type == "hub":
        return types.ModelHubParams(**params)
    elif model_type == "link":
        return types.ModelLinkParams(**params)
    elif model_type == "t_link":
        return types.ModelTLinkParams(**params)
    elif model_type == "sat":
        return types.ModelSatParams(**params)
    elif model_type == "eff_sat":
        return types.ModelEffSatParams(**params)
    elif model_type == "ma_sat":
        return types.ModelMaSatParams(**params)
    elif model_type == "xts":
        return types.ModelXtsParams(**params)
    elif model_type == "pit":
        return types.ModelPitParams(**params)
    elif model_type == "bridge":
        return types.ModelBridgeParams(**params)
    elif model_type not in get_args(types.DBTVaultModel):
        raise ValueError(f"Model_type {model_type} made it into model_param_factory")
    raise ValueError(f"Model type {model_type} not currently supported")


def parse_model_definition(
    model_dict: types.Mapping,
    defaults: types.Mapping,
    config_path: str,
) -> types.DBTVGBaseModelParams:
    # Get the options for the group
    options = model_dict.get(literals.DBTVG_OPTIONS_KEY, {})
    model_dict[literals.DBTVG_OPTIONS_KEY] = build_model_config(
        defaults, options, config_path
    )
    # Update default location - this is where it is found, not where it's going
    model_dict[literals.DBTVG_LOCATION_KEY] = config_path

    name = model_dict.get(literals.DBTVG_NAME_KEY, "--NAME MISSING--")
    error_prefix = f"The param object at location {config_path} with name {name} has"

    model_type = model_dict.get(literals.DBTVG_MODEL_TYPE_KEY, None)
    if model_type not in get_args(types.DBTVaultModel) or model_type is None:
        raise exceptions.DBTVaultConfigInvalidError(
            f"{error_prefix} has invalid type {str(model_type)}"
        )
    try:
        return model_param_factory(model_type, model_dict)
    except pydantic.ValidationError as ve:
        # If validation fails, pass up a truncated error message to the cli

        errors = [item.get("loc") for item in ve.errors()]
        raise exceptions.DBTVaultConfigInvalidError(
            f"{error_prefix} raised validation errors on the following fields:"
            f"{str(errors)}"
        )


def process_config_collection(configs: Dict[str, types.Mapping]):
    # Use root config defaults to update all child defaults,
    # else use global defaults
    if len(configs) == 0:
        raise exceptions.NoDbtvGenConfigFound(
            f"No files with the name {literals.DBTVG_YAML_NAME} found in specified "
            "directory, please check metadata has been correctly specified"
        )
    cfg: types.Mapping = (
        configs["."].get(literals.DBTVG_DEFAULTS_KEY, {}) if "." in configs else {}
    )
    root_defaults = types.DBTVGConfig(**cfg).dict()

    models: List[types.DBTVGBaseModelParams] = []
    for config_loc, local_config in configs.items():
        # Always update the keys - we want to propagate the default values out
        default_config = recursive_merge(
            root_defaults, local_config.get(literals.DBTVG_DEFAULTS_KEY, {})
        )
        # Iterate over each of the files, update the config and append to the
        # dict that will construct the config object. This moves the model configs
        # into their configured form - each is now independent of default configs
        model_configs = [
            parse_model_definition(item, default_config, config_loc)
            for item in local_config.get(literals.DBTVG_MODELS_KEY, [])
        ]
        models.extend(model_configs)

    # Check for duplicate model names
    duplicates = [""]
    check: Dict[str, str] = {}
    for item in models:
        cname = check.get(item.name, "")
        if cname != "":
            duplicates.append(f"{item.name}: {cname} and {item.location}")
        else:
            check[item.name] = item.location
    # Empty string as sentinel value
    if len(duplicates) > 1:
        err_loc = "\n".join(duplicates)
        raise exceptions.DBTVaultConfigInvalidError(
            f"Duplicate model names detected: {err_loc}"
        )

    return models


def check_model_names(arg_string: Optional[str] = None) -> List[str]:
    if arg_string is None:
        return []
    arg_dict = yaml.safe_load(arg_string)
    model_names: Optional[List[str]] = arg_dict.get("model_names", None)
    if model_names is None:
        return []

    # Validate input is a list of strings
    failed = False
    if not isinstance(model_names, list):  # type: ignore
        failed = True
    elif len(model_names) == 0:
        failed = True
    elif not isinstance(model_names[0], str):  # type: ignore
        failed = True
    if failed:
        raise exceptions.ArgParseError(
            "Argument model_names must be a non-empty list of strings"
        )

    return model_names


def build_exec_docgen_command(cli_args: List[str], model_names: List[str]) -> List[str]:
    args_string: str = yaml.dump({"model_names": model_names}, default_flow_style=True)
    args_string = args_string.replace("\n", "")
    base_commands: List[str] = ["dbt", "run-operation", "generate_model_yaml"]
    args = ["--args", args_string]
    return base_commands + cli_args + args


def coerce_yaml_list_to_str(yaml_list: types.YamlStringList) -> str:
    if isinstance(yaml_list, str):
        return yaml_list
    else:
        first_elem = next(iter(yaml_list))
        warnings.warn(f"Yaml list {str(yaml_list)} will be coerced to {first_elem}")
        return first_elem


def coerce_yaml_list_to_list(yaml_list: types.YamlStringList) -> List[str]:
    if isinstance(yaml_list, str):
        return [yaml_list]
    return yaml_list


class BaseModelRelationshipBuild(abc.ABC):
    @abc.abstractmethod
    def __call__(self, config: Any) -> types.RelationshipExtract:
        pass


class HubModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelHubParams) -> types.RelationshipExtract:
        relationships = types.RelationshipExtract(
            primary_key=coerce_yaml_list_to_str(config.dbtvault_arguments.src_pk),
        )
        return relationships


class LinkModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelLinkParams) -> types.RelationshipExtract:
        relationships = types.RelationshipExtract(
            primary_key=coerce_yaml_list_to_str(config.dbtvault_arguments.src_pk),
            foreign_keys=config.dbtvault_arguments.src_fk,
        )
        return relationships


class TLinkModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelTLinkParams) -> types.RelationshipExtract:
        relationships = types.RelationshipExtract(
            primary_key=coerce_yaml_list_to_str(config.dbtvault_arguments.src_pk),
            foreign_keys=config.dbtvault_arguments.src_fk,
        )
        return relationships


class SatModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelSatParams) -> types.RelationshipExtract:
        relationships = types.RelationshipExtract(
            foreign_keys=[config.dbtvault_arguments.src_pk],
        )
        return relationships


class EffSatModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelEffSatParams) -> types.RelationshipExtract:
        relationships = types.RelationshipExtract(
            foreign_keys=[
                coerce_yaml_list_to_str(config.dbtvault_arguments.src_pk),
                coerce_yaml_list_to_str(config.dbtvault_arguments.src_dfk),
                coerce_yaml_list_to_str(config.dbtvault_arguments.src_sfk),
            ],
        )
        return relationships


class MaSatModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelMaSatParams) -> types.RelationshipExtract:
        relationships = types.RelationshipExtract(
            foreign_keys=[config.dbtvault_arguments.src_pk]
            + coerce_yaml_list_to_list(config.dbtvault_arguments.src_cdk),
        )
        return relationships


class XtsModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelXtsParams) -> types.RelationshipExtract:
        relationships = types.RelationshipExtract(
            foreign_keys=coerce_yaml_list_to_list(config.dbtvault_arguments.src_pk),
        )
        return relationships


class PitModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelPitParams) -> types.RelationshipExtract:
        satellite_keys = [
            value.pk["PK"] for value in config.dbtvault_arguments.satellites.values()
        ]
        relationships = types.RelationshipExtract(
            primary_key=config.dbtvault_arguments.src_pk,
            foreign_keys=satellite_keys,
        )
        return relationships


class BridgeModelRelationBuild(BaseModelRelationshipBuild):
    def __call__(self, config: types.ModelHubParams) -> types.RelationshipExtract:
        # TODO: Don't be a lazy bastard, come back to this
        warnings.warn("Bridge walks currently hurts my head, I am passing")
        return types.RelationshipExtract()


def model_relations_factory(
    model_type: types.DBTVaultModel,
) -> BaseModelRelationshipBuild:
    if model_type == "hub":
        return HubModelRelationBuild()
    elif model_type == "link":
        return LinkModelRelationBuild()
    elif model_type == "t_link":
        return TLinkModelRelationBuild()
    elif model_type == "sat":
        return SatModelRelationBuild()
    elif model_type == "eff_sat":
        return EffSatModelRelationBuild()
    elif model_type == "ma_sat":
        return MaSatModelRelationBuild()
    elif model_type == "xts":
        return XtsModelRelationBuild()
    elif model_type == "pit":
        return PitModelRelationBuild()
    elif model_type == "bridge":
        return BridgeModelRelationBuild()
    else:
        raise ValueError(
            f"Model type {model_type} passed into relationship builder factory"
        )


def build_primary_keys(
    models: List[Tuple[str, types.DBTVGBaseModelParams]]
) -> Dict[str, str]:
    """Extract all hub primary keys as the foundation of relationship matching"""
    primary_keys: Dict[str, str] = {}
    for name, model in models:
        primary_key = None
        if model.model_type in ["hub", "link", "t_link"]:
            relation_extract = model_relations_factory(model.model_type)
            primary_key = relation_extract(model).primary_key

        if primary_key is None:
            continue
        if primary_key in primary_keys:
            warnings.warn(
                f"Duplicate hub primary key name {primary_key} found, "
                "automatic generation will be inconsistent"
            )
        else:
            primary_keys[primary_key] = name
    return primary_keys


def match_foreign_keys(
    models: List[Tuple[str, types.DBTVGBaseModelParams]],
    primary_key_dict: Dict[str, str],
) -> Dict[str, Dict[str, str]]:
    foreign_key_match: Dict[str, Dict[str, str]] = {}
    for name, model in models:
        foreign_key_match[name] = {}
        relation_extract = model_relations_factory(model.model_type)
        for item in relation_extract(model).foreign_keys:
            if item in primary_key_dict:
                # Current table, shared key denoting rel'p, primary table
                foreign_key_match[name] = {item: primary_key_dict[item]}

    return foreign_key_match


def find_model_relationships(
    models: List[Tuple[str, types.DBTVGBaseModelParams]]
) -> Dict[str, Dict[str, str]]:
    """Use greedy matching to find relationships between primary or foreign keys"""
    primary_keys = build_primary_keys(models)
    return match_foreign_keys(models, primary_keys)


def primary_key_test() -> List[str]:
    return ["unique", "not_null"]


def foreign_key_test(pk_table: str, column_name: str) -> List[types.Mapping]:
    """Designs a check that will never fire so foreign keys can be inferred"""
    non_alert = {"config": {"where": "1 != 1"}}
    fk_check = {
        "relationships": {
            "to": f"ref('{pk_table}')",
            "field": column_name,
            "config": non_alert,
        }
    }
    return [fk_check]


def build_relationship_entry(
    model: types.DBTVGBaseModelParams,
    catalog_model: types.CatalogModel,
    relationship_record: Dict[str, str],
) -> types.Mapping:
    primary_key = model_relations_factory(model.model_type)(model).primary_key
    output: types.Mapping = {
        "name": catalog_model.name,
        "description": "",
    }
    column_holder: List[types.Mapping] = []
    for name, item in catalog_model.columns.items():
        test_holder: List[Union[types.Mapping, str]] = []
        column: types.Mapping = {
            "name": name,
            "description": "",
            "data_type": item.dtype,
        }
        if name == primary_key:
            test_holder.extend(primary_key_test())
        elif name in relationship_record:
            test_holder.extend(foreign_key_test(relationship_record[name], name))
        if len(test_holder) > 0:
            column["tests"] = test_holder
        column_holder.append(column)

    output["columns"] = column_holder
    return output
