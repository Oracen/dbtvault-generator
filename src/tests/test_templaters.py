import unittest
from pathlib import Path
from typing import List

from dbtvault_generator.constants import literals, types
from dbtvault_generator.files import file_io
from dbtvault_generator.parsers import templaters

TEST_ROOT = Path(__file__).parent
data_path = TEST_ROOT / "data/input_yml"
test_models = file_io.read_yml_file(
    data_path / "dbt_generator_config_extra.yml",
    TypeError,
    "file not found",
)[literals.DBTVG_CONFIG_KEY][literals.DBTVG_MODELS_KEY]

custom_macro_models = file_io.read_yml_file(
    data_path / "dbt_generator_config_good.yml",
    TypeError,
    "file not found",
)[literals.DBTVG_CONFIG_KEY][literals.DBTVG_MODELS_KEY]


def _search(param_list: List[types.Mapping], key: str) -> types.Mapping:
    options = {
        "use_prefix": False,
        "target_path": ".",
    }
    found = next(item for item in param_list if item["model_type"] == key)
    return {**found, "location": "", "options": options}


class TestDbtvaultTemplaters(unittest.TestCase):
    def test_dbtvault_custom_macro(self):
        params = _search(test_models, "stage")
        options = types.DBTVGConfig(custom_macros={"stage": "blah_macro"})
        args: types.Mapping = {**params, "options": options}
        stage_model = types.ModelStageParams(**args)
        templater = templaters.templater_factory(stage_model.model_type)
        self.assertFalse("none" in templater(stage_model))
        self.assertTrue("blah_macro" in templater(stage_model))

    def test_dbtvault_stage_template(self):
        params = _search(test_models, "stage")
        stage_model = types.ModelStageParams(**params)
        templater = templaters.templater_factory(stage_model.model_type)
        self.assertFalse("none" in templater(stage_model))
        self.assertTrue("dbtvault.stage" in templater(stage_model))

    def test_dbtvault_hub_template(self):
        params = _search(test_models, "hub")
        model = types.ModelHubParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.hub" in templater(model))

    def test_dbtvault_link_template(self):
        params = _search(test_models, "link")
        model = types.ModelLinkParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.link" in templater(model))

    def test_dbtvault_t_link_template(self):
        params = _search(test_models, "t_link")
        model = types.ModelTLinkParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.t_link" in templater(model))

    def test_dbtvault_sat_template(self):
        params = _search(test_models, "sat")
        model = types.ModelSatParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.sat" in templater(model))

    def test_dbtvault_eff_sat_template(self):
        params = _search(test_models, "eff_sat")
        model = types.ModelEffSatParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.eff_sat" in templater(model))

    def test_dbtvault_ma_sat_template(self):
        params = _search(test_models, "ma_sat")
        model = types.ModelMaSatParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.ma_sat" in templater(model))

    def test_dbtvault_xts_template(self):
        params = _search(test_models, "xts")
        model = types.ModelXtsParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.xts" in templater(model))

    def test_dbtvault_pit_template(self):
        params = _search(test_models, "pit")
        model = types.ModelPitParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.pit" in templater(model))

    def test_dbtvault_bridge_template(self):
        params = _search(test_models, "bridge")
        model = types.ModelBridgeParams(**params)
        templater = templaters.templater_factory(model.model_type)
        self.assertFalse("none" in templater(model))
        self.assertTrue("dbtvault.bridge" in templater(model))
