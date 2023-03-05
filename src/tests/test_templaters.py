import unittest
from pathlib import Path
from typing import Any, Dict

from dbtvault_generator.constants import literals, types
from dbtvault_generator.files import file_io
from dbtvault_generator.parsers import templaters

TEST_ROOT = Path(__file__).parent
data_path = TEST_ROOT / "data/input_yml"
good_yml = file_io.read_yml_file(
    data_path / "dbt_generator_config_good.yml",
    TypeError,
    "file not found",
)[literals.DBTVG_CONFIG_KEY]


class TestTemplaters(unittest.TestCase):
    def test_basic_yaml_coercion(self):
        params: Dict[str, Any] = {**good_yml["models"][0], "location": ""}
        stage_model = types.DBTVGModelStageParams(**params)
        templater = templaters.templater_factory(stage_model.model_type)
        self.assertFalse("none" in templater(stage_model))
