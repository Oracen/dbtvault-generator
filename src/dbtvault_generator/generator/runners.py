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


class SqlGenerator(BaseGenerator):
    def run(
        self,
        args_dict: types.Mapping,
    ) -> None:
        project_path: Path = args_dict.get("project_dir", Path("."))
        params.arg_handler(args_dict.pop("args", None))
        params.cli_passthrough_arg_parser(args_dict)
        project_config = self.get_project_config_fn(project_path)

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
        for model_config in models:
            templater = templater_factory(model_config.model_type)
            template_string = templater(model_config)
            # Allow for prefixing to be dynamic
            prefix = (
                model_config.options.prefixes[model_config.model_type]
                if model_config.options.use_prefix
                else ""
            )
            name = f"{prefix}{model_config.name}.{literals.SQL_FILE_EXT}"
            file_location = Path(project_config / model_config.options.target_path)
            file_location.mkdir(parents=True, exist_ok=True)
            file_io.write_text(file_location / name, template_string)
