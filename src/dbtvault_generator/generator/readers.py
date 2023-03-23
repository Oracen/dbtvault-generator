from pathlib import Path
from typing import Dict, List

from dbtvault_generator.constants import exceptions, literals, types
from dbtvault_generator.files import search
from dbtvault_generator.parsers import params


class ConfigReader:
    def __init__(self, reader_function: types.ReaderFunction):
        self.reader_function = reader_function

    def readin_dbtvg_configs(
        self,
        project_dir: Path,
        model_folder: str = "",
        recursive: bool = False,
    ) -> Dict[str, types.Mapping]:
        # Search through the specified folder for config files
        search_path = project_dir / model_folder
        files = search.find_files(search_path, literals.DBTVG_YAML_NAME, recursive)
        # Strip the leading path
        keys = [
            "."
            + str(item)
            .replace(str(project_dir), "")
            .replace(f"/{literals.DBTVG_YAML_NAME}", "")
            for item in files
        ]
        configs: List[types.Mapping] = [
            self.reader_function(
                file,
                exceptions.DBTVaultConfigInvalidError,
                f"Error reading in file {str(file)}",
            )
            for file in files
        ]
        return {
            key: config[literals.DBTVG_CONFIG_KEY]
            for key, config in zip(keys, configs)
            if literals.DBTVG_CONFIG_KEY in config
        }


class ExecEnvReader:
    def __init__(self, subproc_runner_fn: types.ShellOperationFn):
        self.subproc_runner_fn = subproc_runner_fn

    def check_dbt_install(self):
        try:
            self.subproc_runner_fn(["dbt", "--version"])
        except FileNotFoundError:
            raise exceptions.NoDbtInstallError(
                "A DBT install was not found in environment"
            )


class SchemaMerger:
    def __init__(
        self,
        reader_function: types.ReaderFunction,
        writer_function: types.DictWriterFunction,
    ):
        self.reader_function = reader_function
        self.writer_function = writer_function

    def merge_schemas(
        self, target_path: Path, model_payload: types.Mapping, overwrite: bool
    ) -> None:
        # Save and exit, don't need to merge
        if overwrite or not target_path.is_file():
            self.writer_function(target_path, model_payload)
            return

        existing = self.reader_function(
            target_path,
            exceptions.DbtArtifactError,
            f"The existing schema at {str(target_path)} is in an invalid format",
        )
        updated_schema = params.recursive_merge(model_payload, existing)
        self.writer_function(target_path, updated_schema)
