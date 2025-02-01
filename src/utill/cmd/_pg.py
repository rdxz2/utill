def _pg_to_pg(src_profile: str, src_table: str, dst_profile: str, dst_table: str, columns: str):
    from ..my_pg import PG

    columns = ','.join([f"{x}" for x in columns.split(',')]) if columns != '*' else None
    pg_src = PG(src_profile)
    pg_dst = PG(dst_profile)

    pg_src.pg_to_pg(pg_dst, src_table, dst_table, columns)


def _upload_csv(profile: str, src_filename: str, dst_table: str):
    from ..my_pg import PG

    pg = PG(profile)
    pg.upload_csv(src_filename, dst_table)
