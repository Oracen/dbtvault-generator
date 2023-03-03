from contextlib import contextmanager

from click.exceptions import BadOptionUsage, Exit

from dbtvault_generator.constants import exceptions


@contextmanager
def exception_handler():
    try:
        yield Exit(0)
    except exceptions.ArgParseError as ape:
        raise BadOptionUsage("--args", str(ape))
    except exceptions.ProjectNotConfiguredError as pnce:
        raise BadOptionUsage("--project-dir", str(pnce))
