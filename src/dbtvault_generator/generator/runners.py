import abc
from pathlib import Path

from dbtvault_generator.constants import literals, types
from dbtvault_generator.files import file_io
from dbtvault_generator.parsers import params
from dbtvault_generator.parsers.templaters import templater_factory


class BaseGenerator(abc.ABC):
    def __init__(
        self,
        subproc_runner_fn: types.RunOperationFn,
        get_project_config_fn: types.GetProjectConfigFn,
        find_dbtvault_gen_config_fn: types.FindDbtvaultGenConfig,
    ):
        self.subproc_runner_fn = subproc_runner_fn
        self.get_project_config_fn = get_project_config_fn
        self.find_dbtvault_gen_config_fn = find_dbtvault_gen_config_fn

    def run(self, args_dict: types.Mapping):
        pass

    def process_config(self, args_dict: types.Mapping):
        project_path: Path = Path(args_dict.get("project_dir", "."))
        args = params.arg_handler(args_dict.pop("args", None))
        cli_args = params.cli_passthrough_arg_parser(args_dict)
        project_config = self.get_project_config_fn(
            project_path, args_dict.get("target", None)
        )

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
            project_dir=project_path, models=models, args=args, cli_args=cli_args
        )


class SqlGenerator(BaseGenerator):
    def run(
        self,
        args_dict: types.Mapping,
    ) -> None:
        runner_config = self.process_config(args_dict)
        for model_config in runner_config.models:
            templater = templater_factory(model_config.model_type)
            template_string = templater(model_config)
            # Allow for prefixing to be dynamic
            prefix = (
                model_config.options.prefixes[model_config.model_type]
                if model_config.options.use_prefix
                else ""
            )
            name = f"{prefix}{model_config.name}.{literals.SQL_FILE_EXT}"
            file_loc = runner_config.project_dir / model_config.options.target_path

            file_loc.mkdir(parents=True, exist_ok=True)
            file_io.write_text(file_loc / name, template_string)


class DocsGenerator(BaseGenerator):
    def run(
        self,
        args_dict: types.Mapping,
    ) -> None:
        runner_config = self.process_config(args_dict)
        for model_config in runner_config.models:
            templater = templater_factory(model_config.model_type)
            template_string = templater(model_config)
            # Allow for prefixing to be dynamic
            prefix = (
                model_config.options.prefixes[model_config.model_type]
                if model_config.options.use_prefix
                else ""
            )
            name = f"{prefix}{model_config.name}.{literals.SQL_FILE_EXT}"
            file_loc = runner_config.project_dir / model_config.options.target_path

            file_loc.mkdir(parents=True, exist_ok=True)
            file_io.write_text(file_loc / name, template_string)
