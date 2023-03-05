import unittest
from pathlib import Path

from dbtvault_generator.constants import exceptions, literals
from dbtvault_generator.files import file_io
from dbtvault_generator.parsers import params

TEST_ROOT = Path(__file__).parent
data_path = TEST_ROOT / "data/input_yml"


class TestValidate(unittest.TestCase):
    def test_process_config_collection(self):
        good_yml = file_io.read_yml_file(
            data_path / "dbt_generator_config_good.yml",
            TypeError,
            "file not found",
        )[literals.DBTVG_CONFIG_KEY]

        output = params.process_config_collection({"./path/to/file": good_yml})
        # print(output[1])
        # raise ValueError
        self.assertEqual(len(output), 2, "good yaml should only have 2 stage objects")
        self.assertNotEqual(
            output[0].options.target_path,
            output[1].options.target_path,
            "Path specification is being overwritten",
        )

    def test_process_config_collection_composition(self):
        good_yml, extra_yml = (
            file_io.read_yml_file(
                data_path / f"dbt_generator_config_{name}.yml",
                TypeError,
                "file not found",
            )[literals.DBTVG_CONFIG_KEY]
            for name in ["good", "extra"]
        )

        output = params.process_config_collection(
            {".": good_yml, "./models/data_vault": extra_yml}
        )

        self.assertEqual(len(output), 3)
        self.assertTrue(output[-1].options.prefixes)

    def test_process_config_collection_failures(self):
        good_yml, bad_yml = (
            file_io.read_yml_file(
                data_path / f"dbt_generator_config_{name}.yml",
                TypeError,
                "file not found",
            )[literals.DBTVG_CONFIG_KEY]
            for name in ["good", "bad"]
        )
        with self.assertRaises(
            exceptions.DBTVaultConfigInvalidError,
            msg="Parsing should fail if duplicate names are present",
        ):
            params.process_config_collection(
                {
                    ".": good_yml,
                    "./models/data_vault": good_yml,
                }
            )

        with self.assertRaises(
            exceptions.DBTVaultConfigInvalidError,
            msg="Missing fields should yield error",
        ):
            params.process_config_collection(
                {
                    ".": bad_yml,
                }
            )
