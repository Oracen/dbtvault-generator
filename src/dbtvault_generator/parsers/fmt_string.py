from dbtvault_generator.constants import types


def format_name(model_config: types.DBTVGBaseModelParams) -> str:
    prefix = (
        model_config.options.prefixes[model_config.model_type]
        if model_config.options.use_prefix
        else ""
    )
    return f"{prefix}{model_config.name}"
