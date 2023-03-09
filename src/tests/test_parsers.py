import unittest
from pathlib import Path

from dbtvault_generator.constants import exceptions, types
from dbtvault_generator.files import file_io
from dbtvault_generator.parsers import fmt_string, params

TEST_ROOT = Path(__file__).parent


class TestParser(unittest.TestCase):
    def test_arg_yaml_loads(self):
        good_yaml = {1: 2, "a": "b"}
        bad_yml_str = '{1:2", :"b"}'

        self.assertDictEqual(good_yaml, params.arg_handler(str(good_yaml)))
        with self.assertRaises(exceptions.ArgParseError):
            params.arg_handler(bad_yml_str)

    def test_cli_passthough_arg_parser(self):
        want = ["--project-dir", str(Path("path/to/file").absolute())]
        self.assertEqual(want, params.cli_passthrough_arg_parser(Path("path/to/file")))

    def test_get_dbt_project_config(self):
        data_path = TEST_ROOT / "data/projects"
        want = types.ProjectConfig(model_dirs=["models"], target_dir="target")
        self.assertEqual(
            want, params.get_dbt_project_config(data_path / "dummy_project", None)
        )
        with self.assertRaises(exceptions.ProjectNotConfiguredError):
            params.get_dbt_project_config(data_path / "broken_project", None)

    def test_base_doc_parsing(self):
        data_path = TEST_ROOT / "data/text"

        with open(data_path / "doc_gen_output.txt", "r") as stream:
            data = stream.read().replace("\\n", "\n")
        cleaned = fmt_string.clean_generate_model_yaml(data)
        output = file_io.load_base_doc_object(cleaned)
        self.assertTrue("version" in output.dict())
        self.assertTrue("models" in output.dict())
        self.assertEqual(len(output.models), 3)
