import humanize
import math
import os
import shutil

from google.cloud import bigquery, storage
from loguru import logger
from textwrap import dedent

from .my_csv import read_header, combine, compress
from .my_datetime import current_datetime_str
from .my_env import envs
from .my_file import make_sure_path_is_directory, adjust_sep
from .my_gcs import GCS
from .my_queue import ThreadingQ
from .my_string import replace_nonnumeric
from .my_xlsx import csv_to_xlsx


class CsvToBqMode:
    OVERWRITE = 'OVERWRITE'
    APPEND = 'APPEND'

    def __init__(self) -> None:
        pass


class BqDataType:
    INT64 = 'INT64'
    INTEGER = 'INTEGER'
    FLOAT64 = 'FLOAT64'

    DECIMAL = 'DECIMAL'

    STRING = 'STRING'
    JSON = 'JSON'

    DATE = 'DATE'
    TIME = 'TIME'
    DATETIME = 'DATETIME'
    TIMESTAMP = 'TIMESTAMP'

    BOOL = 'BOOL'

    ARRAY_INT64 = 'ARRAY<INT64>'
    ARRAY_INTEGER = 'ARRAY<INTEGER>'
    ARRAY_FLOAT64 = 'ARRAY<FLOAT64>'
    ARRAY_STRING = 'ARRAY<STRING>'
    ARRAY_JSON = 'ARRAY<JSON>'
    ARRAY_DATE = 'ARRAY<DATE>'
    ARRAY_DATETIME = 'ARRAY<DATETIME>'
    ARRAY_TIMESTAMP = 'ARRAY<TIMESTAMP>'
    ARRAY_BOOL = 'ARRAY<BOOL>'


def translate_dict_to_colstring(cols: dict): return ',\n'.join([f'  `{x}` {y}' for x, y in cols.items()])


class BQ():

    def __init__(self, project: str = envs.GCP_PROJECT_ID, disable_print_query: bool = True):
        self.disable_print_query = disable_print_query
        self.client = bigquery.Client(project=project)
        logger.debug(f'BQ client open, project: {project or "<application-default>"}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close_client()

    def execute_query(self, query: str, dry_run: bool = False) -> bigquery.QueryJob:
        multistatement = type(query) == list
        if multistatement:
            query = '\n'.join([x if str(x).strip().endswith(';') else x + ';' for x in query if x])  # noqa
        else:
            query = query.strip()

        if self.disable_print_query:
            logger.debug(f'🔎 Query:\n{query}')
        query_job_config = bigquery.QueryJobConfig(dry_run=dry_run)
        query_job = self.client.query(query, job_config=query_job_config)
        query_job.result()  # wait query execution

        if not multistatement:
            logger.debug(f'[Job ID] {query_job.job_id}, [Processed] {humanize.naturalsize(query_job.total_bytes_processed)}, [Billed] {humanize.naturalsize(query_job.total_bytes_billed)}, [Affected] {query_job.num_dml_affected_rows or 0} row(s)',)
        else:
            logger.debug(f'[Job ID] {query_job.job_id}')
            [logger.debug(f'[Script ID] {job.job_id}, [Processed] {humanize.naturalsize(job.total_bytes_processed)}, [Billed] {humanize.naturalsize(job.total_bytes_billed)}, [Affected] {job.num_dml_affected_rows or 0} row(s)',) for job in self.client.list_jobs(parent_job=query_job.job_id)]

        return query_job

    def load_data_into(self, table: str, gcs_path: list[str] | str, cols_str: str, partition_by: str = None, cluster_by: str = None, overwrite: bool = False):
        if type(gcs_path) == str:
            gcs_path = [gcs_path]
        gcs_path_str = ',\n'.join([f'    \'{x}\'' for x in gcs_path])

        load_data_keyword = 'OVERWRITE' if overwrite else 'INTO'
        query = dedent(
            f'''
            LOAD DATA {load_data_keyword} `{table}` (
            {cols_str}
            )
            {f"PARTITION BY {partition_by}" if partition_by is not None else "-- No partition key"}
            {f"CLUSTER BY {cluster_by}" if cluster_by is not None else "-- No cluster key"}
            FROM FILES(
                skip_leading_rows=1,
                allow_quoted_newlines=true,
                format='csv',
                compression='gzip',
                uris = [
            {gcs_path_str}
                ]
            );
            '''
        )
        return self.execute_query(query, return_df=False)

    def export_data(self, query: str, gcs_path: str, pre_query: str = None):
        if '*' not in gcs_path:
            raise ValueError('GCS path need to have a single \'*\' wildcard character')  # noqa

        query = dedent(
            f'''
            EXPORT DATA OPTIONS (
                uri='{gcs_path}',
                format='csv',
                compression='gzip',
                overwrite=true,
                header=true,
                field_delimiter=',')
            AS (
            {query}
            );
            '''
        )

        if pre_query:
            query = [pre_query, query]

        return self.execute_query(query, return_df=False)

    def download_csv(
        self,
        query: str,
        file_path: str,
        is_combine: bool = True,
        pre_query: str = None,
    ):
        file_path = adjust_sep(os.path.expanduser(file_path))

        make_sure_path_is_directory(file_path)

        if os.path.exists(file_path):
            shutil.rmtree(file_path)

        os.makedirs(file_path, exist_ok=True)

        current_time = current_datetime_str()
        gcs_path = f'gs://{envs.GCS_BUCKET}/tmp/{current_time}__unload_query/*.csv.gz'

        logger.info('Export data...')
        self.export_data(query, gcs_path, pre_query)

        gcs = GCS()
        logger.info('Downloads from GCS...')
        file_paths = []
        for blob in gcs.list(f'tmp/{current_time}__unload_query/'):
            file_path_part = os.path.join(file_path, blob.name.split('/')[-1])
            gcs.download(blob, file_path_part)
            file_paths.append(file_path_part)

        if is_combine:
            logger.info('Combine downloaded csv...')
            file_path_final = f'{file_path[:-1]}.csv'
            combine(file_paths, file_path_final)
            shutil.rmtree(file_path)

    def upload_csv(
        self,
        file_path: str,
        table: str,
        cols: dict,
        partition_by: str = None,
        cluster_by: str = None,
        mode: CsvToBqMode = CsvToBqMode.APPEND,
    ):
        file_path = adjust_sep(file_path)

        if not file_path.endswith('.csv'):
            raise ValueError('Please provide file path with .csv extension!')

        if partition_by is not None:
            if partition_by not in cols.keys():
                raise ValueError(f'Partition \'{partition_by}\' not exists in columns!')
        if cluster_by is not None:
            if cluster_by not in cols.keys():
                raise ValueError(f'Cluster \'{cluster_by}\' not exists in columns!')

        # Build list of columns with its datatypes
        csv_cols = set(read_header(file_path))
        excessive_cols = set(cols.keys()) - set(csv_cols)
        if excessive_cols:
            raise ValueError(f'{len(excessive_cols)} columns not exists in CSV file: {", ".join(excessive_cols)}')  # noqa
        nonexistent_cols = set(csv_cols) - set(cols.keys())
        if nonexistent_cols:
            raise ValueError(f'{len(nonexistent_cols)} columns from CSV are missing: {", ".join(nonexistent_cols)}')  # noqa
        cols_str = translate_dict_to_colstring(cols)

        gcs = GCS(envs.GCS_BUCKET.split('/')[0])
        tmp_dir = f'tmp/{current_datetime_str()}__upload'

        def producer(src_file: str):
            for dst_file in compress(src_file, keep=True):
                yield (dst_file, )

        def consumer(dst_file: str):
            remote_file_name = f'{tmp_dir}/{replace_nonnumeric(os.path.basename(dst_file), "_").lower()}.csv.gz'
            logger.debug(f'Uploading {dst_file} to {remote_file_name}...')
            blob = gcs.upload(dst_file, remote_file_name, mv=True)
            return blob

        blobs: list[storage.Blob]
        _, blobs = ThreadingQ().add_producer(producer, file_path).add_consumer(consumer).execute()

        try:
            logger.debug('Load data into...')
            if mode == CsvToBqMode.OVERWRITE:
                self.load_data_into(table, [f'gs://{blob.bucket.name}/{blob.name}' for blob in blobs], cols_str, overwrite=True, partition_by=partition_by, cluster_by=cluster_by)
            elif mode == CsvToBqMode.APPEND:
                self.load_data_into(table, [f'gs://{blob.bucket.name}/{blob.name}' for blob in blobs], cols_str, partition_by=partition_by, cluster_by=cluster_by)
            else:
                return ValueError(f'Data insertion mode not recognized: {mode}')
        except Exception as e:
            raise e
        finally:
            [GCS.remove_blob(blob) for blob in blobs]

        gcs.close_client()

    def bq_to_xlsx(
        self,
        table_name: str,
        file_path: str,
        limit: int = 950000,
    ):
        if file_path.endswith(os.sep):
            raise ValueError(f'Please provide file path NOT ending with \'{os.sep}\' character')

        table_name_tmp = f'{table_name}_'
        self.execute_query(f'CREATE TABLE `{table_name_tmp}` AS SELECT *, ROW_NUMBER() OVER() AS _rn FROM `{table_name}`', return_df=False)

        try:
            cnt = self.execute_query(f'SELECT COUNT(1) AS cnt FROM `{table_name}`')['cnt'].values[0]
            parts = math.ceil(cnt / limit)
            logger.debug(f'Total part: {cnt} / {limit} = {parts}')
            for part in range(parts):
                logger.debug(f'Download part {part + 1}...')
                file_path_tmp = f'{file_path}_part{part + 1}'
                file_path_tmp_csv = f'{file_path_tmp}.csv'
                self.download_csv(f'SELECT * EXCEPT(_rn) FROM `{table_name_tmp}` WHERE _rn BETWEEN {(part * limit) + 1} AND {(part + 1) * limit}', f'{file_path_tmp}{os.sep}')
                logger.debug('CSV into XLXS...')
                # df_to_xlsx([pd.read_csv(file_path_tmp_csv)], [sheet_name], f'{file_path_tmp}.xlsx')
                csv_to_xlsx(file_path_tmp_csv, f'{file_path_tmp}.xlsx')
                os.remove(file_path_tmp_csv)
        except Exception as e:
            raise e
        finally:
            self.execute_query(f'DROP TABLE IF EXISTS `{table_name_tmp}`', return_df=False)

    def copy_table(self, src_table_id: str, dst_table_id: str, drop: bool = False):
        # Create or replace
        self.client.delete_table(dst_table_id, not_found_ok=True)
        self.client.copy_table(src_table_id, dst_table_id).result()
        logger.debug(f'Table {src_table_id} copied to {dst_table_id}')

        if drop:
            self.client.delete_table(src_table_id)
            logger.debug(f'Table {src_table_id} dropped')

    def copy_view(self, src_view_id: str, dst_view_id: str, drop: bool = False):
        src_project_id, src_dataset_id, _ = src_view_id.split('.')
        dst_project_id, dst_dataset_id, _ = dst_view_id.split('.')

        # Create or replace
        src_view = self.client.get_table(src_view_id)
        dst_view = bigquery.Table(dst_view_id)
        dst_view.view_query = src_view.view_query.replace(f'{src_project_id}.{src_dataset_id}', f'{dst_project_id}.{dst_dataset_id}')
        self.client.delete_table(dst_view, not_found_ok=True)
        self.client.create_table(dst_view)
        logger.debug(f'View {src_view_id} copied to {dst_view}')

        if drop:
            self.client.delete_table(src_view_id)
            logger.debug(f'View {src_view_id} dropped')

    def copy_routine(self, src_routine_id: str, dst_routine_id: str, drop: bool = False):
        src_project_id, src_dataset_id, _ = src_routine_id.split('.')
        dst_project_id, dst_dataset_id, _ = dst_routine_id.split('.')

        # Create or replace
        src_routine = self.client.get_routine(src_routine_id)
        dst_routine = bigquery.Routine(dst_routine_id)
        dst_routine.body = src_routine.body.replace(f'{src_project_id}.{src_dataset_id}', f'{dst_project_id}.{dst_dataset_id}')
        dst_routine.type_ = src_routine.type_
        dst_routine.description = src_routine.description
        dst_routine.language = src_routine.language
        dst_routine.arguments = src_routine.arguments
        dst_routine.return_type = src_routine.return_type
        self.client.delete_routine(dst_routine, not_found_ok=True)
        self.client.create_routine(dst_routine)
        logger.debug(f'Routine {src_routine_id} copied to {dst_routine_id}')

        if drop:
            self.client.delete_routine(src_routine_id)
            logger.debug(f'Routine {src_routine_id} dropped')

    def close_client(self):
        self.client.close()
        logger.debug('BQ client close')
