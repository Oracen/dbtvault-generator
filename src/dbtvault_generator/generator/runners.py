import abc
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, List, Optional, Tuple

from dbtvault_generator.constants import literals, types
from dbtvault_generator.files import file_io
from dbtvault_generator.parsers import fmt_string, params
from dbtvault_generator.parsers.templaters import templater_factory


class BaseGenerator(abc.ABC):
    def __init__(
        self,
        get_project_config_fn: types.GetProjectConfigFn,
        find_dbtvault_gen_config_fn: types.FindDbtvaultGenConfig,
    ):
        self.get_project_config_fn = get_project_config_fn
        self.find_dbtvault_gen_config_fn = find_dbtvault_gen_config_fn

    def process_config(self, project_path: Path, target_folder: Optional[str]):
        cli_args = params.cli_passthrough_arg_parser(project_path, target_folder)
        project_config = self.get_project_config_fn(project_path, target_folder)

        # Load in configs, starting  with root config
        configs = self.find_dbtvault_gen_config_fn(project_path, "", False)
        # Next, iterate over all folders configured for models, and pull in any configs
        # that are there
        for model_folder in set(project_config.model_dirs):
            configs.update(
                self.find_dbtvault_gen_config_fn(project_path, model_folder, True)
            )
        # Run through all the files and builds the sql as appropriate
        models = params.process_config_collection(configs)
        return types.RunnerConfig(
            project_dir=project_path,
            models=models,
            cli_args=cli_args,
            target_folder=project_config.target_dir,
        )


class SqlGenerator(BaseGenerator):
    def run(
        self,
        project_path: Path,
        overwrite: bool = False,
    ) -> None:
        runner_config = self.process_config(project_path, None)
        for model_config in runner_config.models:
            templater = templater_factory(model_config.model_type)
            template_string = templater(model_config)

            name = f"{fmt_string.format_name(model_config)}.{literals.SQL_FILE_EXT}"
            file_loc = runner_config.project_dir / model_config.options.target_path

            file_loc.mkdir(parents=True, exist_ok=True)
            filepath = file_loc / name
            if filepath.is_file() and not overwrite:
                continue
                # Don't overwrite existing
            file_io.write_text(file_loc / name, template_string)


class DocsGenerator(BaseGenerator):
    def __init__(
        self,
        get_project_config_fn: types.GetProjectConfigFn,
        find_dbtvault_gen_config_fn: types.FindDbtvaultGenConfig,
        subproc_runner_fn: types.ShellOperationFn,
        schema_file_merger: types.SchemaMergeFn,
    ):
        super().__init__(get_project_config_fn, find_dbtvault_gen_config_fn)
        self.subproc_runner_fn = subproc_runner_fn
        self.schema_file_merger = schema_file_merger

    def run(
        self,
        project_path: Path,
        target_folder: Optional[str] = None,
        args: Optional[str] = None,
        overwrite: bool = False,
    ) -> None:
        runner_config = self.process_config(project_path, target_folder)
        model_names = params.check_model_names(args)
        target_dir = runner_config.project_dir / runner_config.target_folder
        catalog_data = file_io.load_catalog(target_dir)
        model_list = (
            runner_config.models
            if len(model_names) == 0
            else list(filter(lambda x: x.name in model_names, runner_config.models))
        )
        model_namepairs: List[Tuple[str, types.DBTVGBaseModelParams]] = [
            (fmt_string.format_name(item), item) for item in model_list
        ]
        # command_list = params.build_exec_docgen_command(
        #     runner_config.cli_args, model_names
        # )
        # output_str = fmt_string.clean_generate_model_yaml(
        #     self.subproc_runner_fn(command_list)
        # )

        # base_doc_data = params.load_base_doc_object(output_str)
        relationship_data = params.find_model_relationships(model_namepairs)
        model_locations: DefaultDict[str, List[types.Mapping]] = defaultdict(list)
        for name, model in model_namepairs:
            relationship = relationship_data[name]
            catalog_model = catalog_data.models[name]
            data_entry = params.build_relationship_entry(
                model, catalog_model, relationship
            )
            model_locations[model.options.target_path].append(data_entry)
        filename = literals.DEFAULT_NAME_SCHEMA_YAML
        for location, models in model_locations.items():
            model_payload = {"version": 2, "models": models}
            target_file = runner_config.project_dir / location / filename
            self.schema_file_merger(target_file, model_payload, overwrite)
