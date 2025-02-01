def _upload_csv(src_filename: str, dst_table_fqn: str, cols: list[tuple[str, str]], partition_col: str = None, cluster_cols: list[str] = None, project: str = None):
    from ..my_bq import BQ

    bq = BQ(project)
    bq.upload_csv(src_filename, dst_table_fqn, {col: dtype for col, dtype in cols}, partition_col, cluster_cols)


def _download_table(src_table_fqn: str, dst_filename: str, project: str):
    from ..my_bq import BQ

    bq = BQ(project)
    bq.download_csv(f'SELECT * FROM {src_table_fqn}', dst_filename)
