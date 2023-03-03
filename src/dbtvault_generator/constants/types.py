from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type

import pydantic

DBTVaultModel = Literal[
    "stage", "hub", "link", "t_link", "sat", "eff_sat", "ma_sat", "xts", "pit", "bridge"
]
Mapping = Dict[str, Any]


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


class DBTVGModelParams(pydantic.BaseModel):
    name: str
    location: str
    options: DBTVGConfig


class DBTVStageParams(pydantic.BaseModel):
    include_source_columns: bool = True
    source_model: Mapping
    derived_columns: Optional[Mapping] = None
    hashed_columns: Optional[Mapping] = None
    ranked_columns: Optional[Mapping] = None


class DBTVGModelStageParams(DBTVGModelParams):
    dbtvault_arguments: DBTVStageParams


class DBTVGModels(pydantic.BaseModel):
    stage: List[DBTVGModelParams]


PipeOutput = Tuple[str, str, bool]
RunOperationFn = Callable[[str, List[str]], PipeOutput]
GetProjectConfigFn = Callable[[Path], ProjectConfig]
FindDbtvaultGenConfig = Callable[[Path, str, bool], Dict[str, Mapping]]
ReaderFunction = Callable[[Path, Type[Exception], str], Mapping]
