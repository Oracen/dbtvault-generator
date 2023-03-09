class ArgParseError(TypeError):
    pass


class ProjectNotConfiguredError(TypeError):
    pass


class DBTVaultConfigInvalidError(ValueError):
    pass


class NoDbtInstallError(ImportError):
    pass
