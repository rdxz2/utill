import click


@click.group()
def main(): pass


# Conf


@main.group('conf', help='Configure this library')
def main__conf(): pass
@main__conf.command('init', help='Initialize env files')
@click.argument('mode', type=click.Choice(['google-cloud', 'postgresql', 'metabase']))
def main__conf__init(**kwargs): from ._conf import _init; _init(**kwargs)
@main__conf.command('list', help='List all configs')
def main__conf__list(): from ._conf import _list; _list()
@main__conf.command('set', help='Set configuration variables')
@click.option('-e', 'vars', type=(str, str), multiple=True, required=True, help='Variables -> K V')
def main__conf__set(**kwargs): from ._conf import _set; _set(**kwargs)


# Gen


@main.group('gen', help='Generation utility')
def main__gen(): pass
@main__gen.command('random-string', help='Generate random string')
@click.option('-l', 'length', type=int, default=32, help='Length of the random string')
@click.option('-a', 'alphanum', is_flag=True, default=False, help='Use alphanumeric only (a-Z, 0-9)')
def gen_random_string(**kwargs): from ._gen import _random_string; _random_string(**kwargs)


# PG


@main.group('pg', help='PostgreSQL utility')
def main__pg(): pass
@main__pg.command('pg-to-pg', help='Copy table from one PG instance to another')
@click.argument('src_profile', type=str)
@click.argument('src_table', type=str)
@click.argument('dst_profile', type=str)
@click.argument('dst_table', type=str)
@click.option('-c', '--columns', type=str, default='*', help='Columns to copy')
def main__pg__pg_to_pg(**kwargs): from ._pg import _pg_to_pg; _pg_to_pg(**kwargs)
@main__pg.command('upload-csv', help='Upload local CSV')
@click.argument('profile', type=str)
@click.argument('src_csv', type=click.Path())
@click.argument('dst_table', type=str)
def main__pg__upload_csv(**kwargs): from ._pg import _upload_csv; _upload_csv(**kwargs)


if __name__ == '__main__':
    main()
