import abc
from typing import Any, get_args

import yaml

from dbtvault_generator.constants import types

spc = "        "
endset = "{%- endset -%}"
set_yaml_metadata = "{%- set yaml_metadata -%}"
set_metadata_dict = "{% set metadata_dict = fromyaml(yaml_metadata) %}"
render_left = "{{  "
render_right = "  }}"


def inject_yaml_metadata(dbtvault_parameters: types.Mapping) -> str:
    yaml_string = yaml.dump(
        dbtvault_parameters, default_flow_style=False, sort_keys=False
    )
    code = f"""{set_yaml_metadata}

{yaml_string}

{endset}

{set_metadata_dict}
"""
    return code


def format_metadata_lookup(params: Any, name: str) -> str:
    # getattr will deliberately fail if there's no matching object
    value = "none" if getattr(params, name) is None else f"metadata_dict['{name}']"
    return f"{name}={value}"


def dbtvault_stage(stage_params: types.DBTVStageParams) -> str:
    code = f"""dbtvault.stage(
{spc}{format_metadata_lookup(stage_params, 'include_source_columns')},
{spc}{format_metadata_lookup(stage_params, 'source_model')},
{spc}{format_metadata_lookup(stage_params, 'derived_columns')},
{spc}{format_metadata_lookup(stage_params, 'null_columns')},
{spc}{format_metadata_lookup(stage_params, 'hashed_columns')},
{spc}{format_metadata_lookup(stage_params, 'ranked_columns')},
)"""
    return f"""{render_left}{code}{render_right}"""


class BaseTemplater(abc.ABC):
    @abc.abstractmethod
    def __call__(self, config: types.DBTVGModelParams) -> str:
        pass


class ModelStageTemplater(BaseTemplater):
    def __call__(self, params: types.DBTVGModelStageParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_stage(params.dbtvault_arguments)}"""


def templater_factory(config_type: types.DBTVaultModel) -> BaseTemplater:
    if config_type == "stage":
        return ModelStageTemplater()
    elif config_type not in get_args(types.DBTVaultModel):
        raise ValueError(f"Config Type {config_type} somehow in templater_factory")
    raise ValueError(f"Model type {config_type} not supported")
