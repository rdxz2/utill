import click


@click.group()
def main(): pass


# <<----- START: Conf


@main.group('conf', help='Configure utill library')
def main__conf(): pass
@main__conf.command('init', help='Initialize env files')
def main__conf__init(): from ._conf import _init; _init()
@main__conf.command('list', help='List all configs')
def main__conf__list(): from ._conf import _list; _list()
@main__conf.command('set', help='Set configuration variables')
@click.option('-e', 'vars', type=(str, str), multiple=True, required=True, help='Variables -> K V')
def main__conf__set(**kwargs): from ._conf import _set; _set(**kwargs)


# END: Conf ----->>


if __name__ == '__main__':
    main()
