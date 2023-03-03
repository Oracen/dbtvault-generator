from .cli import dbtvgen, handlers


def main():
    with handlers.exception_handler():
        dbtvgen()


if __name__ == "__main__":
    main()
