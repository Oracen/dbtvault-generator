from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type, Union

import pydantic

DBTVaultModel = Literal[
    "stage", "hub", "link", "t_link", "sat", "eff_sat", "ma_sat", "xts", "pit", "bridge"
]
YamlStringList = Union[List[str], str]
Mapping = Dict[str, Any]


class ProjectConfig(pydantic.BaseModel):
    model_dirs: List[str]
    target_dir: str


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
    table: DBTVGTablePrefixes = DBTVGTablePrefixes()


class DBTVGConfig(pydantic.BaseModel):
    use_prefix: bool = False
    target_path: str = ""
    prefixes: DBTVGPrefixes = DBTVGPrefixes()
    custom_macros: Dict[DBTVaultModel, str] = {}


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


class RunnerConfig(pydantic.BaseModel):
    project_dir: Path
    models: List[DBTVGBaseModelParams]
    cli_args: List[str]
    target_folder: str


class CatalogModelColumn(pydantic.BaseModel):
    name: str
    dtype: Optional[str]


class CatalogModel(pydantic.BaseModel):
    name: str
    columns: Dict[str, CatalogModelColumn]


class DbtCatalog(pydantic.BaseModel):
    models: Dict[str, CatalogModel]


class RelationshipExtract(pydantic.BaseModel):
    primary_key: Optional[str] = None
    foreign_keys: List[str] = []


class DocgenForeignKey(pydantic.BaseModel):
    table: str
    field: str


class DocgenBaseColumn(pydantic.BaseModel):
    name: str
    description: str
    is_primary_key: bool
    foreign_key: Optional[DocgenForeignKey]


class DocgenBaseTable(pydantic.BaseModel):
    name: str
    description: str
    columns: List[DocgenBaseColumn]


class DocgenModels(pydantic.BaseModel):
    version: int
    models: List[DocgenBaseTable]


PipeOutput = Tuple[str, str, bool]
ShellOperationFn = Callable[[List[str]], str]
GetProjectConfigFn = Callable[[Path, Optional[str]], ProjectConfig]
FindDbtvaultGenConfig = Callable[[Path, str, bool], Dict[str, Mapping]]
ReaderFunction = Callable[[Path, Type[Exception], str], Mapping]
DictWriterFunction = Callable[[Path, Mapping], None]
StringWriterFunction = Callable[[Path, str], None]
SchemaMergeFn = Callable[[Path, Mapping, bool], None]
CatalogLoadFn = Callable[[Path], DbtCatalog]
