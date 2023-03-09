import abc
from copy import deepcopy
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
    dbtvault_parameters = deepcopy(dbtvault_parameters)
    param_iterator = list(dbtvault_parameters.items())
    for key, value in param_iterator:
        if value is None:
            dbtvault_parameters.pop(key)

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


def dbtvault_template_stage(stage_params: types.StageParams) -> str:
    code = f"""dbtvault.stage(
{spc}{format_metadata_lookup(stage_params, 'include_source_columns')},
{spc}{format_metadata_lookup(stage_params, 'source_model')},
{spc}{format_metadata_lookup(stage_params, 'derived_columns')},
{spc}{format_metadata_lookup(stage_params, 'null_columns')},
{spc}{format_metadata_lookup(stage_params, 'hashed_columns')},
{spc}{format_metadata_lookup(stage_params, 'ranked_columns')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_hub(hub_params: types.HubParams) -> str:
    code = f"""dbtvault.hub(
{spc}{format_metadata_lookup(hub_params, 'src_pk')},
{spc}{format_metadata_lookup(hub_params, 'src_nk')},
{spc}{format_metadata_lookup(hub_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(hub_params, 'src_ldts')},
{spc}{format_metadata_lookup(hub_params, 'src_source')},
{spc}{format_metadata_lookup(hub_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_link(link_params: types.LinkParams) -> str:
    code = f"""dbtvault.link(
{spc}{format_metadata_lookup(link_params, 'src_pk')},
{spc}{format_metadata_lookup(link_params, 'src_fk')},
{spc}{format_metadata_lookup(link_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(link_params, 'src_ldts')},
{spc}{format_metadata_lookup(link_params, 'src_source')},
{spc}{format_metadata_lookup(link_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_t_link(t_link_params: types.TLinkParams) -> str:
    code = f"""dbtvault.t_link(
{spc}{format_metadata_lookup(t_link_params, 'src_pk')},
{spc}{format_metadata_lookup(t_link_params, 'src_fk')},
{spc}{format_metadata_lookup(t_link_params, 'src_payload')},
{spc}{format_metadata_lookup(t_link_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(t_link_params, 'src_eff')},
{spc}{format_metadata_lookup(t_link_params, 'src_ldts')},
{spc}{format_metadata_lookup(t_link_params, 'src_source')},
{spc}{format_metadata_lookup(t_link_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_sat(sat_params: types.SatParams) -> str:
    code = f"""dbtvault.sat(
{spc}{format_metadata_lookup(sat_params, 'src_pk')},
{spc}{format_metadata_lookup(sat_params, 'src_hashdiff')},
{spc}{format_metadata_lookup(sat_params, 'src_payload')},
{spc}{format_metadata_lookup(sat_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(sat_params, 'src_eff')},
{spc}{format_metadata_lookup(sat_params, 'src_ldts')},
{spc}{format_metadata_lookup(sat_params, 'src_source')},
{spc}{format_metadata_lookup(sat_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_eff_sat(eff_sat_params: types.EffSatParams) -> str:
    code = f"""dbtvault.eff_sat(
{spc}{format_metadata_lookup(eff_sat_params, 'src_pk')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_dfk')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_sfk')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_start_date')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_end_date')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_eff')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_ldts')},
{spc}{format_metadata_lookup(eff_sat_params, 'src_source')},
{spc}{format_metadata_lookup(eff_sat_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_ma_sat(ma_sat_params: types.MaSatParams) -> str:
    code = f"""dbtvault.ma_sat(
{spc}{format_metadata_lookup(ma_sat_params, 'src_pk')},
{spc}{format_metadata_lookup(ma_sat_params, 'src_cdk')},
{spc}{format_metadata_lookup(ma_sat_params, 'src_hashdiff')},
{spc}{format_metadata_lookup(ma_sat_params, 'src_payload')},
{spc}{format_metadata_lookup(ma_sat_params, 'src_eff')},
{spc}{format_metadata_lookup(ma_sat_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(ma_sat_params, 'src_ldts')},
{spc}{format_metadata_lookup(ma_sat_params, 'src_source')},
{spc}{format_metadata_lookup(ma_sat_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_xts(xts_params: types.XtsParams) -> str:
    code = f"""dbtvault.xts(
{spc}{format_metadata_lookup(xts_params, 'src_pk')},
{spc}{format_metadata_lookup(xts_params, 'src_satellite')},
{spc}{format_metadata_lookup(xts_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(xts_params, 'src_ldts')},
{spc}{format_metadata_lookup(xts_params, 'src_source')},
{spc}{format_metadata_lookup(xts_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_pit(pit_params: types.PitParams) -> str:
    code = f"""dbtvault.pit(
{spc}{format_metadata_lookup(pit_params, 'src_pk')},
{spc}{format_metadata_lookup(pit_params, 'as_of_dates_table')},
{spc}{format_metadata_lookup(pit_params, 'satellites')},
{spc}{format_metadata_lookup(pit_params, 'stage_tables_ldts')},
{spc}{format_metadata_lookup(pit_params, 'src_ldts')},
{spc}{format_metadata_lookup(pit_params, 'source_model')}
)"""
    return f"""{render_left}{code}{render_right}"""


def dbtvault_template_bridge(bridge_params: types.BridgeParams) -> str:
    code = f"""dbtvault.bridge(
{spc}{format_metadata_lookup(bridge_params, 'source_model')},
{spc}{format_metadata_lookup(bridge_params, 'src_pk')},
{spc}{format_metadata_lookup(bridge_params, 'src_ldts')},
{spc}{format_metadata_lookup(bridge_params, 'bridge_walk')},
{spc}{format_metadata_lookup(bridge_params, 'as_of_dates_table')},
{spc}{format_metadata_lookup(bridge_params, 'stage_tables_ldts')}
)"""
    return f"""{render_left}{code}{render_right}"""


class BaseTemplater(abc.ABC):
    @abc.abstractmethod
    def __call__(self, config: Any) -> str:
        pass


class ModelStageTemplater(BaseTemplater):
    def __call__(self, params: types.ModelStageParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_stage(params.dbtvault_arguments)}"""


class ModelHubTemplater(BaseTemplater):
    def __call__(self, params: types.ModelHubParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_hub(params.dbtvault_arguments)}"""


class ModelLinkTemplater(BaseTemplater):
    def __call__(self, params: types.ModelLinkParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_link(params.dbtvault_arguments)}"""


class ModelTLinkTemplater(BaseTemplater):
    def __call__(self, params: types.ModelTLinkParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_t_link(params.dbtvault_arguments)}"""


class ModelSatTemplater(BaseTemplater):
    def __call__(self, params: types.ModelSatParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_sat(params.dbtvault_arguments)}"""


class ModelEffSatTemplater(BaseTemplater):
    def __call__(self, params: types.ModelEffSatParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_eff_sat(params.dbtvault_arguments)}"""


class ModelMaSatTemplater(BaseTemplater):
    def __call__(self, params: types.ModelMaSatParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_ma_sat(params.dbtvault_arguments)}"""


class ModelXtsTemplater(BaseTemplater):
    def __call__(self, params: types.ModelXtsParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_xts(params.dbtvault_arguments)}"""


class ModelPitTemplater(BaseTemplater):
    def __call__(self, params: types.ModelPitParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_pit(params.dbtvault_arguments)}"""


class ModelBridgeTemplater(BaseTemplater):
    def __call__(self, params: types.ModelBridgeParams) -> str:
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_bridge(params.dbtvault_arguments)}"""


def templater_factory(config_type: types.DBTVaultModel) -> BaseTemplater:
    if config_type == "stage":
        return ModelStageTemplater()
    elif config_type == "hub":
        return ModelHubTemplater()
    elif config_type == "link":
        return ModelLinkTemplater()
    elif config_type == "t_link":
        return ModelTLinkTemplater()
    elif config_type == "sat":
        return ModelSatTemplater()
    elif config_type == "eff_sat":
        return ModelEffSatTemplater()
    elif config_type == "ma_sat":
        return ModelMaSatTemplater()
    elif config_type == "xts":
        return ModelXtsTemplater()
    elif config_type == "pit":
        return ModelPitTemplater()
    elif config_type == "bridge":
        return ModelBridgeTemplater()
    elif config_type not in get_args(types.DBTVaultModel):
        raise ValueError(f"Config Type {config_type} somehow in templater_factory")
    raise ValueError(f"Model type {config_type} not supported")
