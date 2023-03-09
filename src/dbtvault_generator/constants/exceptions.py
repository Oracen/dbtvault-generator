class ArgParseError(TypeError):
    pass


class ProjectNotConfiguredError(TypeError):
    pass


class DBTVaultConfigInvalidError(ValueError):
    pass


class NoDbtInstallError(ImportError):
    pass


class NoDbtvGenConfigFound(ValueError):
    pass


class SubprocessFailed(ValueError):
    pass


class DbtArtifactError(TypeError):
    pass
