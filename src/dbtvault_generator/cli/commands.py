import click

from dbtvault_generator.constants import types
from dbtvault_generator.files import file_io
from dbtvault_generator.generator import readers, runners, subprocess
from dbtvault_generator.parsers import params
from dbtvault_generator.wrappers import dbt_cli as dbtvgen
from dbtvault_generator.wrappers import dbt_params

# @dbtvgen.command(
#     "sql", context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
# )
# @click.pass_context
# @dbt_params.args
# @dbt_params.profile
# @dbt_params.profiles_dir
# @dbt_params.project_dir
# @dbt_params.target
# @dbt_params.vars
# def sql(ctx: click.Context, **kwargs: Mapping) -> None:
#     """
#     Run the generation from `dbtvault.yml` to sql, per model directory
#     """
#     print(ctx.obj)
#     print(ctx.args)
#     # print(ctx)

#     print(kwargs)
#     runners.run_sql_generator(subprocess.run_dbtvault_operation, kwargs)


@dbtvgen.command(
    "debug", context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
@click.pass_context
@dbt_params.args
@dbt_params.profile
@dbt_params.profiles_dir
@dbt_params.project_dir
@dbt_params.target
@dbt_params.vars
def debug(ctx: click.Context, **kwargs: types.Mapping) -> None:
    """
    Run the generation from `dbtvault.yml` to sql, per model directory
    """
    print(ctx.obj)
    print(ctx.args)
    # print(ctx)

    print(kwargs)

    config_file_reader = readers.ConfigReader(file_io.read_yml_file)
    job_runner = runners.SqlGenerator(
        subprocess.run_dbtvault_operation,
        params.get_dbt_project_config,
        config_file_reader.readin_dbtvg_configs,
    )
    job_runner.run(kwargs)


if __name__ == "__main__":
    dbtvgen()
