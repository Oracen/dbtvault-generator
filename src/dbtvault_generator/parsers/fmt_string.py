from dbtvault_generator.constants import types


def format_name(model_config: types.DBTVGBaseModelParams) -> str:
    assert model_config.options.prefixes is not None
    prefix = (
        getattr(model_config.options.prefixes.table, model_config.model_type)
        if model_config.options.use_prefix
        else ""
    )
    return f"{prefix}{model_config.name}"
