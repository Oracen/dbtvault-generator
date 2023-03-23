from pathlib import Path
from typing import Optional

import typer

from dbtvault_generator.files import file_io
from dbtvault_generator.generator import readers, runners, subprocess
from dbtvault_generator.parsers import params

dbtvgen = typer.Typer(pretty_exceptions_show_locals=False)

param_project_dir: Path = typer.Option(  # type: ignore
    Path.cwd(),
    help="The location of the target project",
)

param_target_folder: str = typer.Option(  # type: ignore
    None,
    help="The location of the target folder where DBT artifacts are saved",
)

param_args_yaml: str = typer.Option(  # type: ignore
    None,
    help=(
        "A dbt-codegen compliant yaml string with 'model_names' as a key, used to "
        "specify which models to generate documentation for"
    ),
)

param_args_overwrite: bool = typer.Option(  # type: ignore
    False,
    "--overwrite",
    help=("Specifies whether to overwrite any existing files"),
)


@dbtvgen.command()
def sql(
    ctx: typer.Context,
    project_path: Path = param_project_dir,
    overwrite: bool = param_args_overwrite,
) -> None:
    """
    Run the generation from `dbtvault.yml` to sql, per model directory
    """
    config_file_reader = readers.ConfigReader(file_io.read_yml_file)

    job_runner = runners.SqlGenerator(
        params.get_dbt_project_config,
        config_file_reader.readin_dbtvg_configs,
        file_io.write_text,
    )
    job_runner.run(project_path, overwrite)


@dbtvgen.command()
def docs(
    ctx: typer.Context,
    project_path: Path = param_project_dir,
    target_folder: Optional[str] = param_target_folder,
    args: Optional[str] = param_args_yaml,
    overwrite: bool = param_args_overwrite,
) -> None:
    """
    Scan for all available metadata to augment any existing documentation
    """
    # # Check for install first
    # readers.ExecEnvReader(subprocess.run_shell_operation).check_dbt_install()

    # Configure job runner
    config_file_reader = readers.ConfigReader(file_io.read_yml_file)
    schema_merge_file = readers.SchemaMerger(
        file_io.read_yml_file, file_io.write_yaml_file
    )
    job_runner = runners.DocsGenerator(
        params.get_dbt_project_config,
        config_file_reader.readin_dbtvg_configs,
        subprocess.run_shell_operation,
        schema_merge_file.merge_schemas,
        file_io.load_catalog,
    )
    job_runner.run(project_path, target_folder, args, overwrite)


@dbtvgen.command()
def debug(
    ctx: typer.Context,
    project_path: Path = param_project_dir,
    target_folder: Optional[str] = param_target_folder,
) -> None:
    """
    Displays the kwargs and context
    """
    typer.echo(ctx.obj)
    typer.echo(ctx.args)
    typer.echo(f"project_path: {str(project_path)}")
    typer.echo(f"target_folder: {str(target_folder)}")


if __name__ == "__main__":
    dbtvgen()
