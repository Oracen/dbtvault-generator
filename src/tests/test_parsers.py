import unittest
from pathlib import Path, PosixPath

from dbtvault_generator.constants import exceptions, types
from dbtvault_generator.parsers import params

TEST_ROOT = Path(__file__).parent


class TestParser(unittest.TestCase):
    def test_arg_yaml_loads(self):
        good_yaml = {1: 2, "a": "b"}
        bad_yml_str = '{1:2", :"b"}'

        self.assertDictEqual(good_yaml, params.arg_handler(str(good_yaml)))
        with self.assertRaises(exceptions.ArgParseError):
            params.arg_handler(bad_yml_str)

    def test_cli_passthough_arg_parser(self):
        want = ["--kebab-case-arg", "value", "--path", "path/to/file"]
        test_case = {"kebab_case_arg": "value", "path": PosixPath("path/to/file")}
        self.assertEqual(want, params.cli_passthrough_arg_parser(test_case))

    def test_get_dbt_project_config(self):
        data_path = TEST_ROOT / "data/projects"
        want = types.ProjectConfig(model_dirs=["models"])
        self.assertEqual(
            want, params.get_dbt_project_config(data_path / "dummy_project")
        )
        with self.assertRaises(exceptions.ProjectNotConfiguredError):
            params.get_dbt_project_config(data_path / "broken_project")
