from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, get_args

import pydantic
import yaml
from yaml.parser import ParserError

from dbtvault_generator.constants import exceptions, literals, types
from dbtvault_generator.files import file_io


def recursive_merge(base: types.Mapping, updated: types.Mapping) -> types.Mapping:
    """Recursively merges 2 dicts, one on top of the other"""
    base = deepcopy(base)  # Base is really our "default config"
    keys: Set[str] = set(base)
    new_keys = set(updated)
    for key in keys:
        val: Any = base[key]
        if key in updated:
            if type(val) == dict:
                if type(updated[key] == dict):
                    recursive_merge(base[key], updated[key])
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
    options = model_dict.get(literals.DBTVG_OPTIONS_KEY, {})
    model_dict[literals.DBTVG_OPTIONS_KEY] = build_model_config(
        defaults, options, config_path
    )
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

    # Validate input
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
    args_string: str = yaml.dump(
        {"model_namessss": model_names}, default_flow_style=True
    )
    args_string = args_string.replace("\n", "")
    base_commands: List[str] = ["dbt", "run-operation", "generate_model_yaml"]
    args = ["--args", args_string]
    return base_commands + cli_args + args
