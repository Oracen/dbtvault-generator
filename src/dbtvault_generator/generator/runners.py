import abc
from pathlib import Path
from typing import get_args

from dbtvault_generator.constants import types
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
        for model_type in get_args(types.DBTVaultModel):
            templater = templater_factory(model_type)
            for model_config in getattr(models, model_type):
                templater(model_config)
