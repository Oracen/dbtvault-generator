import abc
from pathlib import Path
from typing import Optional

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
        )


class SqlGenerator(BaseGenerator):
    def run(self, project_path: Path) -> None:
        runner_config = self.process_config(project_path, None)
        for model_config in runner_config.models:
            templater = templater_factory(model_config.model_type)
            template_string = templater(model_config)

            name = f"{fmt_string.format_name(model_config)}.{literals.SQL_FILE_EXT}"
            file_loc = runner_config.project_dir / model_config.options.target_path

            file_loc.mkdir(parents=True, exist_ok=True)
            file_io.write_text(file_loc / name, template_string)


class DocsGenerator(BaseGenerator):
    def __init__(
        self,
        get_project_config_fn: types.GetProjectConfigFn,
        find_dbtvault_gen_config_fn: types.FindDbtvaultGenConfig,
        subproc_runner_fn: types.ShellOperationFn,
    ):
        super().__init__(get_project_config_fn, find_dbtvault_gen_config_fn)
        self.subproc_runner_fn = subproc_runner_fn

    def run(
        self,
        project_path: Path,
        target_folder: Optional[str] = None,
        args: Optional[str] = None,
    ) -> None:
        print("HI")
        runner_config = self.process_config(project_path, target_folder)
        model_names = params.check_model_names(args)
        if len(model_names) == 0:
            # Default to full generation
            model_names = [
                fmt_string.format_name(item) for item in runner_config.models
            ]

        command_list = params.build_exec_docgen_command(
            runner_config.cli_args, model_names
        )
        print(command_list)
        print(self.subproc_runner_fn(command_list))
