import click


@click.group()
def main():
    pass


# MARK: Settings


@main.group("setting", help="Configure this library")
def main__setting():
    pass


@main__setting.command("init", help="Initialize env files")
@click.argument("mode", type=click.Choice(["google-cloud", "postgresql", "metabase"]))
def main__setting__init(**kwargs):
    from ._setting import _init

    _init(**kwargs)


@main__setting.command("list", help="List all configs")
@click.option(
    "-m",
    "module",
    type=click.Choice(["postgresql", "metabase"]),
    help="List config for a specific modules",
)
def main__setting__list(**kwargs):
    from ._setting import _list

    _list(**kwargs)


@main__setting.command("set", help="Set configuration variables")
@click.option(
    "-e", "vars", type=(str, str), multiple=True, required=True, help="Variables -> K V"
)
def main__setting__set(**kwargs):
    from ._setting import _set

    _set(**kwargs)


# MARK: PG


@main.group("pg", help="PostgreSQL utility")
def main__pg():
    pass


@main__pg.command("pg-to-pg", help="Copy table from one PG instance to another")
@click.argument("src_profile", type=str)
@click.argument("src_table", type=str)
@click.argument("dst_profile", type=str)
@click.argument("dst_table", type=str)
@click.option("-c", "--columns", type=str, default="*", help="Columns to copy")
def main__pg__pg_to_pg(**kwargs):
    from ._postgres import _pg_to_pg

    _pg_to_pg(**kwargs)


@main__pg.command("upload-csv", help="Upload CSV file into PG table")
@click.argument("profile", type=str)
@click.argument("src_filename", type=click.Path())
@click.argument("dst_table", type=str)
def main__pg__upload_csv(**kwargs):
    from ._postgres import _upload_csv

    _upload_csv(**kwargs)


# MARK: BQ


@main.group("bq", help="BigQuery utility")
def main__bq():
    pass


@main__bq.command("upload-csv", help="Upload CSV file into BQ table")
@click.argument("src_filename", type=click.Path())
@click.argument("dst_table_fqn", type=str)
@click.option(
    "-c",
    "columns",
    type=(str, str),
    required=True,
    multiple=True,
    help="Columns -> Name DataType",
)
@click.option("--partition-col", "partition_col", type=str, help="Partition column")
@click.option(
    "--cluster-col", "cluster_cols", type=str, multiple=True, help="Cluster column(s)"
)
@click.option("--project", type=str, help="Billing project")
def main__bq__upload_csv(**kwargs):
    from ._bigquery import _upload_csv

    _upload_csv(**kwargs)


@main__bq.command("download-table", help="Download a BQ table into CSV file")
@click.argument("src_table_fqn", type=str)
@click.argument("dst_filename", type=str)
@click.option("--project", type=str, help="Billing project")
def main__bq__download_table(**kwargs):
    from ._bigquery import _download_table

    _download_table(**kwargs)


# MARK: Encyrption


@main.group("enc", help="Encryption utility")
def main__enc():
    pass


@main__enc.command("encrypt", help="Encrypt a string / file")
@click.argument("src", type=str)
@click.option("-p", "password", type=str, required=True, help="The password")
def main__enc__encrypt(**kwargs):
    from ._encryption import _encrypt

    _encrypt(**kwargs)


# MARK: Other utilities


@main.command("random", help="Generate random string")
@click.option("-l", "length", type=int, default=32, help="Length of the random string")
@click.option(
    "-a",
    "alphanum",
    is_flag=True,
    default=False,
    help="Use alphanumeric only (a-Z, 0-9)",
)
def main__random(**kwargs):
    from ._main import _random

    _random(**kwargs)


@main.command("unique", help="Get unique values")
@click.argument("strings", nargs=-1)
@click.option("-s", "sort", type=bool, is_flag=True, help="Sort the output")
def main__unique(**kwargs):
    from ._main import _unique

    _unique(**kwargs)


if __name__ == "__main__":
    main()
