import abc
from copy import deepcopy
from typing import Any, Optional, get_args

import yaml

from dbtvault_generator.constants import types

spc = "        "
endset = "{%- endset -%}"
set_yaml_metadata = "{%- set yaml_metadata -%}"
set_metadata_dict = "{% set metadata_dict = fromyaml(yaml_metadata) %}"
inj_left = "{{"
inj_right = "}}"


def clean_jinja_syntax(yaml_string: str) -> str:
    if inj_left in yaml_string:
        # Only do jinja replacement if necessary
        yaml_string = yaml_string.replace(f"'{inj_left}", inj_left).replace(
            f"{inj_right}'", inj_right
        )
        yaml_string = yaml_string.replace(f'"{inj_left}', inj_left).replace(
            f'{inj_right}"', inj_right
        )
    return yaml_string


def inject_yaml_metadata(dbtvault_parameters: types.Mapping) -> str:
    dbtvault_parameters = deepcopy(dbtvault_parameters)
    param_iterator = list(dbtvault_parameters.items())
    for key, value in param_iterator:
        if value is None:
            dbtvault_parameters.pop(key)

    yaml_string = yaml.dump(
        dbtvault_parameters, default_flow_style=False, sort_keys=False
    )
    yaml_string = clean_jinja_syntax(yaml_string)
    code = f"""{set_yaml_metadata}

{yaml_string}
{endset}

{set_metadata_dict}"""
    return code


def render_macro(code: str) -> str:
    return f"""{inj_left}
{code}
{inj_right}"""


def format_metadata_lookup(params: Any, name: str) -> str:
    # getattr will deliberately fail if there's no matching object
    value = "none" if getattr(params, name) is None else f"metadata_dict['{name}']"
    return f"{name}={value}".replace("'{{", "{{")


def dbtvault_template_stage(
    stage_params: types.StageParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.stage" if macro is None else macro
    code = f"""{macro}(
{spc}{format_metadata_lookup(stage_params, 'include_source_columns')},
{spc}{format_metadata_lookup(stage_params, 'source_model')},
{spc}{format_metadata_lookup(stage_params, 'derived_columns')},
{spc}{format_metadata_lookup(stage_params, 'null_columns')},
{spc}{format_metadata_lookup(stage_params, 'hashed_columns')},
{spc}{format_metadata_lookup(stage_params, 'ranked_columns')}
)"""
    return render_macro(code)


def dbtvault_template_hub(
    hub_params: types.HubParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.hub" if macro is None else macro
    code = f"""{macro}(
{spc}{format_metadata_lookup(hub_params, 'src_pk')},
{spc}{format_metadata_lookup(hub_params, 'src_nk')},
{spc}{format_metadata_lookup(hub_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(hub_params, 'src_ldts')},
{spc}{format_metadata_lookup(hub_params, 'src_source')},
{spc}{format_metadata_lookup(hub_params, 'source_model')}
)"""
    return render_macro(code)


def dbtvault_template_link(
    link_params: types.LinkParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.link" if macro is None else macro
    code = f"""{macro}(
{spc}{format_metadata_lookup(link_params, 'src_pk')},
{spc}{format_metadata_lookup(link_params, 'src_fk')},
{spc}{format_metadata_lookup(link_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(link_params, 'src_ldts')},
{spc}{format_metadata_lookup(link_params, 'src_source')},
{spc}{format_metadata_lookup(link_params, 'source_model')}
)"""
    return render_macro(code)


def dbtvault_template_t_link(
    t_link_params: types.TLinkParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.t_link" if macro is None else macro
    code = f"""{macro}(
{spc}{format_metadata_lookup(t_link_params, 'src_pk')},
{spc}{format_metadata_lookup(t_link_params, 'src_fk')},
{spc}{format_metadata_lookup(t_link_params, 'src_payload')},
{spc}{format_metadata_lookup(t_link_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(t_link_params, 'src_eff')},
{spc}{format_metadata_lookup(t_link_params, 'src_ldts')},
{spc}{format_metadata_lookup(t_link_params, 'src_source')},
{spc}{format_metadata_lookup(t_link_params, 'source_model')}
)"""
    return render_macro(code)


def dbtvault_template_sat(
    sat_params: types.SatParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.sat" if macro is None else macro
    code = f"""{macro}(
{spc}{format_metadata_lookup(sat_params, 'src_pk')},
{spc}{format_metadata_lookup(sat_params, 'src_hashdiff')},
{spc}{format_metadata_lookup(sat_params, 'src_payload')},
{spc}{format_metadata_lookup(sat_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(sat_params, 'src_eff')},
{spc}{format_metadata_lookup(sat_params, 'src_ldts')},
{spc}{format_metadata_lookup(sat_params, 'src_source')},
{spc}{format_metadata_lookup(sat_params, 'source_model')}
)"""
    return render_macro(code)


def dbtvault_template_eff_sat(
    eff_sat_params: types.EffSatParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.eff_sat" if macro is None else macro
    code = f"""{macro}(
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
    return render_macro(code)


def dbtvault_template_ma_sat(
    ma_sat_params: types.MaSatParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.ma_sat" if macro is None else macro
    code = f"""{macro}(
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
    return render_macro(code)


def dbtvault_template_xts(
    xts_params: types.XtsParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.xts" if macro is None else macro
    code = f"""{macro}(
{spc}{format_metadata_lookup(xts_params, 'src_pk')},
{spc}{format_metadata_lookup(xts_params, 'src_satellite')},
{spc}{format_metadata_lookup(xts_params, 'src_extra_columns')},
{spc}{format_metadata_lookup(xts_params, 'src_ldts')},
{spc}{format_metadata_lookup(xts_params, 'src_source')},
{spc}{format_metadata_lookup(xts_params, 'source_model')}
)"""
    return render_macro(code)


def dbtvault_template_pit(
    pit_params: types.PitParams, macro: Optional[str] = None
) -> str:
    macro = "dbtvault.pit" if macro is None else macro
    code = f"""{macro}(
{spc}{format_metadata_lookup(pit_params, 'src_pk')},
{spc}{format_metadata_lookup(pit_params, 'as_of_dates_table')},
{spc}{format_metadata_lookup(pit_params, 'satellites')},
{spc}{format_metadata_lookup(pit_params, 'stage_tables_ldts')},
{spc}{format_metadata_lookup(pit_params, 'src_ldts')},
{spc}{format_metadata_lookup(pit_params, 'source_model')}
)"""
    return render_macro(code)


def dbtvault_template_bridge(bridge_params: types.BridgeParams, macro: str) -> str:
    code = f"""{macro}(
{spc}{format_metadata_lookup(bridge_params, 'source_model')},
{spc}{format_metadata_lookup(bridge_params, 'src_pk')},
{spc}{format_metadata_lookup(bridge_params, 'src_ldts')},
{spc}{format_metadata_lookup(bridge_params, 'bridge_walk')},
{spc}{format_metadata_lookup(bridge_params, 'as_of_dates_table')},
{spc}{format_metadata_lookup(bridge_params, 'stage_tables_ldts')}
)"""
    return render_macro(code)


class BaseTemplater(abc.ABC):
    @abc.abstractmethod
    def __call__(self, config: Any) -> str:
        pass


class ModelStageTemplater(BaseTemplater):
    default_macro = "dbtvault.stage"

    def __call__(self, params: types.ModelStageParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)

        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_stage(params.dbtvault_arguments, macro)}"""


class ModelHubTemplater(BaseTemplater):
    default_macro = "dbtvault.hub"

    def __call__(self, params: types.ModelHubParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_hub(params.dbtvault_arguments, macro)}"""


class ModelLinkTemplater(BaseTemplater):
    default_macro = "dbtvault.link"

    def __call__(self, params: types.ModelLinkParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)

        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_link(params.dbtvault_arguments, macro)}"""


class ModelTLinkTemplater(BaseTemplater):
    default_macro = "dbtvault.t_link"

    def __call__(self, params: types.ModelTLinkParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_t_link(params.dbtvault_arguments, macro)}"""


class ModelSatTemplater(BaseTemplater):
    default_macro = "dbtvault.sat"

    def __call__(self, params: types.ModelSatParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_sat(params.dbtvault_arguments, macro)}"""


class ModelEffSatTemplater(BaseTemplater):
    default_macro = "dbtvault.eff_sat"

    def __call__(self, params: types.ModelEffSatParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_eff_sat(params.dbtvault_arguments, macro)}"""


class ModelMaSatTemplater(BaseTemplater):
    default_macro = "dbtvault.ma_sat"

    def __call__(self, params: types.ModelMaSatParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_ma_sat(params.dbtvault_arguments, macro)}"""


class ModelXtsTemplater(BaseTemplater):
    default_macro = "dbtvault.xts"

    def __call__(self, params: types.ModelXtsParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_xts(params.dbtvault_arguments, macro)}"""


class ModelPitTemplater(BaseTemplater):
    default_macro = "dbtvault.pit"

    def __call__(self, params: types.ModelPitParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_pit(params.dbtvault_arguments, macro)}"""


class ModelBridgeTemplater(BaseTemplater):
    default_macro = "dbtvault.bridge"

    def __call__(self, params: types.ModelBridgeParams) -> str:
        macro = params.options.custom_macros.get(params.model_type, self.default_macro)
        return f"""{inject_yaml_metadata(params.dbtvault_arguments.dict())}

{dbtvault_template_bridge(params.dbtvault_arguments, macro)}"""


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
