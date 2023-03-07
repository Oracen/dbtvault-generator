from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type, Union

import pydantic

DBTVaultModel = Literal[
    "stage", "hub", "link", "t_link", "sat", "eff_sat", "ma_sat", "xts", "pit", "bridge"
]
YamlStringList = Union[List[str], str]
Mapping = Dict[str, YamlStringList]


class ProjectConfig(pydantic.BaseModel):
    model_dirs: List[str]


class DBTVGTablePrefixes(pydantic.BaseModel):
    stage: str = "stg_"
    hub: str = "hub_"
    link: str = "lnk_"
    t_link: str = "tlnk_"
    sat: str = "sat_"
    eff_sat: str = "eff_sat_"
    ma_sat: str = "ma_sat_"
    xts: str = "xts_"
    pit: str = "pit_"
    bridge: str = "bridge_"


class DBTVGPrefixes(pydantic.BaseModel):
    table: DBTVGTablePrefixes


class DBTVGConfig(pydantic.BaseModel):
    use_prefix: bool = False
    target_path: str = ""
    prefixes: Optional[DBTVGPrefixes] = None
    custom_macros: Optional[Dict[DBTVaultModel, str]] = None


class DBTVGBaseModelParams(pydantic.BaseModel):
    name: str
    model_type: DBTVaultModel
    location: str
    options: DBTVGConfig = DBTVGConfig()


class DBTVaultSrcSatellite(pydantic.BaseModel):
    sat_name: Mapping
    hashdiff: Mapping


class DBTVaultSatellite(pydantic.BaseModel):
    pk: Mapping
    ldts: Mapping


class StageParams(pydantic.BaseModel):
    include_source_columns: bool = True
    source_model: Mapping
    derived_columns: Optional[Mapping] = None
    null_columns: Optional[Mapping] = None
    hashed_columns: Optional[Mapping] = None
    ranked_columns: Optional[Mapping] = None


class HubParams(pydantic.BaseModel):
    src_pk: YamlStringList
    src_nk: YamlStringList
    src_extra_columns: Optional[YamlStringList]
    src_ldts: str
    src_source: YamlStringList
    source_model: YamlStringList


class LinkParams(pydantic.BaseModel):
    src_pk: YamlStringList
    src_fk: List[str]
    src_extra_columns: Optional[YamlStringList]
    src_ldts: str
    src_source: YamlStringList
    source_model: YamlStringList


class TLinkParams(pydantic.BaseModel):
    src_pk: YamlStringList
    src_fk: List[str]
    src_payload: Optional[List[str]]
    src_extra_columns: Optional[YamlStringList]
    src_eff: str
    src_ldts: str
    src_source: str
    source_model: str


class SatParams(pydantic.BaseModel):
    src_pk: str
    src_hashdiff: str
    src_payload: List[str]
    src_extra_columns: Optional[YamlStringList]
    src_eff: Optional[str]
    src_ldts: str
    src_source: str
    source_model: str


class EffSatParams(pydantic.BaseModel):
    src_pk: str
    src_dfk: YamlStringList
    src_sfk: YamlStringList
    src_start_date: str
    src_end_date: str
    src_extra_columns: Optional[YamlStringList]
    src_eff: Optional[str]
    src_ldts: str
    src_source: str
    source_model: str


class MaSatParams(pydantic.BaseModel):
    src_pk: str
    src_cdk: List[str]
    src_hashdiff: str
    src_payload: List[str]
    src_eff: Optional[str]
    src_extra_columns: Optional[YamlStringList]
    src_ldts: str
    src_source: str
    source_model: str


class XtsParams(pydantic.BaseModel):
    src_pk: YamlStringList
    src_satellite: Dict[str, DBTVaultSrcSatellite]
    src_extra_columns: Optional[YamlStringList]
    src_ldts: str
    src_source: str
    source_model: str


class PitParams(pydantic.BaseModel):
    src_pk: str
    as_of_dates_table: str
    satellites: Dict[str, DBTVaultSatellite]
    stage_tables_ldts: Mapping
    src_ldts: str
    source_model: str


class BridgeParams(pydantic.BaseModel):
    source_model: str
    src_pk: str
    src_ldts: str
    bridge_walk: Dict[str, Mapping]
    as_of_dates_table: str
    stage_tables_ldts: Mapping


class ModelStageParams(DBTVGBaseModelParams):
    dbtvault_arguments: StageParams


class ModelHubParams(DBTVGBaseModelParams):
    dbtvault_arguments: HubParams


class ModelLinkParams(DBTVGBaseModelParams):
    dbtvault_arguments: LinkParams


class ModelTLinkParams(DBTVGBaseModelParams):
    dbtvault_arguments: TLinkParams


class ModelSatParams(DBTVGBaseModelParams):
    dbtvault_arguments: SatParams


class ModelEffSatParams(DBTVGBaseModelParams):
    dbtvault_arguments: EffSatParams


class ModelMaSatParams(DBTVGBaseModelParams):
    dbtvault_arguments: MaSatParams


class ModelXtsParams(DBTVGBaseModelParams):
    dbtvault_arguments: XtsParams


class ModelPitParams(DBTVGBaseModelParams):
    dbtvault_arguments: PitParams


class ModelBridgeParams(DBTVGBaseModelParams):
    dbtvault_arguments: BridgeParams


PipeOutput = Tuple[str, str, bool]
RunOperationFn = Callable[[str, List[str]], PipeOutput]
GetProjectConfigFn = Callable[[Path], ProjectConfig]
FindDbtvaultGenConfig = Callable[[Path, str, bool], Dict[str, Mapping]]
ReaderFunction = Callable[[Path, Type[Exception], str], Mapping]
