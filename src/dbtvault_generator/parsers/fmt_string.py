from dbtvault_generator.constants import exceptions, types


def format_name(model_config: types.DBTVGBaseModelParams) -> str:
    assert model_config.options.prefixes is not None
    prefix = (
        getattr(model_config.options.prefixes.table, model_config.model_type)
        if model_config.options.use_prefix
        else ""
    )
    return f"{prefix}{model_config.name}"


def clean_generate_model_yaml(macro_output: str) -> str:
    target_string = "version:"
    if " " + target_string not in macro_output:
        msg = "Unknown format returned from subprocess call, could not format: "
        raise exceptions.SubprocessFailed(msg + str(macro_output))
    # Cut off the printout and extract the yaml component
    return target_string + macro_output.split(target_string, 1)[1]
