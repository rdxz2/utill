import humanize
import math
import os
import shutil

from enum import Enum
from google.cloud import bigquery, storage
from loguru import logger
from textwrap import dedent

from .my_const import ByteSize
from .my_csv import read_header, combine as csv_combine, compress
from .my_datetime import current_datetime_str
from .my_env import envs
from .my_gcs import GCS
from .my_queue import ThreadingQ
from .my_string import replace_nonnumeric
from .my_xlsx import csv_to_xlsx

MAP__PYTHON_DTYPE__BQ_DTYPE = {
    int: 'INTEGER',
    str: 'STRING',
    float: 'STRING',
}


class LoadStrategy(Enum):
    OVERWRITE = 1
    APPEND = 2


class Dtype:
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


class BQ():
    def __init__(self, project: str = None):
        self.project = project or envs.GCP_PROJECT_ID

        self.client = bigquery.Client(project=self.project)
        logger.debug(f'BQ client open, project: {self.project or "<application-default>"}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close_client()

    def execute_query(self, query: str | list[str], dry_run: bool = False, parameters: dict = {}) -> bigquery.QueryJob:
        multi = type(query) == list
        if multi:
            query = '\n'.join([x if str(x).strip().endswith(';') else x + ';' for x in query if x])
        else:
            query = query.strip()

        # Build paramters
        query_parameters = []
        for parameter, value in parameters.items():
            if type(value) == list:
                query_parameters.append(bigquery.ArrayQueryParameter(parameter, MAP__PYTHON_DTYPE__BQ_DTYPE[type(value[0])], value))
            else:
                query_parameters.append(bigquery.ScalarQueryParameter(parameter, MAP__PYTHON_DTYPE__BQ_DTYPE[type(value)], value))

        logger.debug(f'🔎 Query:\n{query}')
        query_job_config = bigquery.QueryJobConfig(dry_run=dry_run, query_parameters=query_parameters)
        query_job = self.client.query(query, job_config=query_job_config)
        query_job.result()  # Wait query execution

        if not multi:
            logger.debug(f'[Job ID] {query_job.job_id}, [Processed] {humanize.naturalsize(query_job.total_bytes_processed)}, [Billed] {humanize.naturalsize(query_job.total_bytes_billed)}, [Affected] {query_job.num_dml_affected_rows or 0} row(s)',)
        else:
            logger.debug(f'[Job ID] {query_job.job_id}')

            jobs: list[bigquery.QueryJob] = self.client.list_jobs(parent_job=query_job.job_id)
            [logger.debug(f'[Script ID] {job.job_id}, [Processed] {humanize.naturalsize(job.total_bytes_processed)}, [Billed] {humanize.naturalsize(job.total_bytes_billed)}, [Affected] {job.num_dml_affected_rows or 0} row(s)',) for job in jobs]

        return query_job

    def create_table(self, bq_table_fqn: str, schema: list[bigquery.SchemaField], partition_col: str, cluster_cols: list[str]):
        table = bigquery.Table(bq_table_fqn, schema=schema)

        if partition_col:
            table.time_partitioning = bigquery.TimePartitioning(field=partition_col)
            table.partitioning_type = 'DAY'

        if cluster_cols:
            table.clustering_fields = cluster_cols

        bq_table = self.client.create_table(table)
        logger.info(f'✅ Table created: {bq_table_fqn}')
        return bq_table

    def drop_table(self, bq_table_fqn: str):
        self.client.delete_table(bq_table_fqn)
        logger.info(f'✅ Table dropped: {bq_table_fqn}')

    def load_data_into(self, bq_table_fqn: str, gcs_path: list[str] | str, cols: dict[str, Dtype], partition_col: str = None, cluster_cols: list[str] = None, overwrite: bool = False):
        if type(gcs_path) == str:
            gcs_path = [gcs_path]
        gcs_path_str = ',\n'.join([f'        \'{x}\'' for x in gcs_path])

        load_data_keyword = 'OVERWRITE' if overwrite else 'INTO'
        cols_str = ',\n'.join([f'    `{x}` {y}' for x, y in cols.items()])
        cluster_cols_str = ','.join([f'`{x}`' for x in cluster_cols]) if cluster_cols else None
        query = dedent(
            f'''
            LOAD DATA {load_data_keyword} `{bq_table_fqn}` (
            {cols_str}
            )
            {f"PARTITION BY `{partition_col}`" if partition_col is not None else "-- No partition column provided"}
            {f"CLUSTER BY {cluster_cols_str}" if cluster_cols_str is not None else "-- No cluster column provided"}
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

        logger.debug(f'⌛ Load data into: {bq_table_fqn}')
        query_job = self.execute_query(query)
        logger.info(f'✅ Load data into: {bq_table_fqn}')
        return query_job

    def export_data(self, query: str, gcs_path: str, pre_query: str = None):
        if '*' not in gcs_path:
            raise ValueError('GCS path need to have a single \'*\' wildcard character')

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

        logger.debug(f'⌛ Export data into: {gcs_path}')
        query_job = self.execute_query(query)
        logger.info(f'✅ Exported data into: {gcs_path}')
        return query_job

    def upload_csv(self, src_filename: str, bq_table_fqn: str, cols: dict[str, Dtype], partition_col: str = None, cluster_cols: list[str] = None, load_strategy: LoadStrategy = LoadStrategy.APPEND):
        # <<----- START: Validation

        if load_strategy not in LoadStrategy:
            raise ValueError('Invalid load strategy')

        if not src_filename.endswith('.csv'):
            raise ValueError('Please provide file path with .csv extension!')

        if partition_col is not None:
            if partition_col not in cols.keys():
                raise ValueError(f'Partition \'{partition_col}\' not exists in columns!')
        if cluster_cols is not None:
            if cluster_cols not in cols.keys():
                raise ValueError(f'Cluster \'{cluster_cols}\' not exists in columns!')

        # Build list of columns with its datatypes
        csv_cols = set(read_header(src_filename))
        excessive_cols = set(cols.keys()) - set(csv_cols)
        if excessive_cols:
            raise ValueError(f'{len(excessive_cols)} columns not exists in CSV file: {", ".join(excessive_cols)}')
        nonexistent_cols = set(csv_cols) - set(cols.keys())
        if nonexistent_cols:
            raise ValueError(f'{len(nonexistent_cols)} columns from CSV are missing: {", ".join(nonexistent_cols)}')

        # END: Validation ----->>

        # <<----- START: Upload to GCS

        gcs = GCS(self.project)
        tmp_dir = f'tmp/upload__{current_datetime_str()}'

        # This will compress while splitting the compressed file to a certain bytes size because of GCS 4GB file limitation
        # A single file can produce more than one compressed file in GCS
        def producer(src_file: str):
            for dst_file in compress(src_file, keep=True, max_size_bytes=ByteSize.GB * 3):
                yield (dst_file, )

        def consumer(dst_file: str):
            remote_file_name = f'{tmp_dir}/{replace_nonnumeric(os.path.basename(dst_file), "_").lower()}.csv.gz'
            logger.debug(f'Uploading {dst_file} to {remote_file_name}...')
            blob = gcs.upload(dst_file, remote_file_name, mv=True)
            return blob

        blobs: list[storage.Blob]
        _, blobs = ThreadingQ().add_producer(producer, src_filename).add_consumer(consumer).execute()

        # END: Upload to GCS ----->>

        # <<----- START: Load to BQ

        try:
            gcs_filename_fqns = [f'gs://{blob.bucket.name}/{blob.name}' for blob in blobs]
            match load_strategy:
                case LoadStrategy.OVERWRITE:
                    self.load_data_into(bq_table_fqn, gcs_filename_fqns, cols, partition_col=partition_col, cluster_cols=cluster_cols, overwrite=True)
                case LoadStrategy.APPEND:
                    self.load_data_into(bq_table_fqn, gcs_filename_fqns, cols, partition_col=partition_col, cluster_cols=cluster_cols)
                case _:
                    return ValueError(f'Load strategy not recognized: {load_strategy}')
        except Exception as e:
            raise e
        finally:
            [GCS.remove_blob(blob) for blob in blobs]

        # END: Load to BQ ----->>

    def download_csv(self, query: str, dst_filename: str, combine: bool = True, pre_query: str = None):
        if not dst_filename.endswith('.csv'):
            raise ValueError('Destination filename must ends with .csv!')
        
        dst_filename = os.path.expanduser(dst_filename)

        dirname = dst_filename.removesuffix('.csv')

        # Remove & recreate existing folder
        if os.path.exists(dirname):
            shutil.rmtree(dirname)
        os.makedirs(dirname, exist_ok=True)

        # Export data into GCS
        current_time = current_datetime_str()
        gcs_path = f'gs://{envs.GCS_BUCKET}/tmp/unload__{current_time}/*.csv.gz'
        self.export_data(query, gcs_path, pre_query)

        # Download into local machine
        gcs = GCS(self.project)
        logger.info('Downloads from GCS...')
        downloaded_filenames = []
        for blob in gcs.list(f'tmp/unload__{current_time}/'):
            file_path_part = os.path.join(dirname, blob.name.split('/')[-1])
            gcs.download(blob, file_path_part)
            downloaded_filenames.append(file_path_part)

        # Combine the file and clean up the file chunks
        if combine:
            logger.info('Combine downloaded csv...')
            csv_combine(downloaded_filenames, dst_filename)
            shutil.rmtree(dirname)

        return dst_filename

    def download_xlsx(self, src_table_fqn: str, dst_filename: str, xlsx_row_limit: int = 950000):
        if not dst_filename.endswith('.xlsx'):
            raise ValueError('Destination filename must ends with .xlsx!')

        # Create a temporary table acting as excel file splitting
        table_name_tmp = f'{src_table_fqn}_'
        self.execute_query(f'CREATE TABLE `{table_name_tmp}` AS SELECT *, ROW_NUMBER() OVER() AS _rn FROM `{src_table_fqn}`')

        try:
            # Calculate the number of excel file parts based on row limit
            cnt = list(self.execute_query(f'SELECT COUNT(1) AS cnt FROM `{src_table_fqn}`').result())[0][0]
            parts = math.ceil(cnt / xlsx_row_limit)
            logger.debug(f'Total part: {cnt} / {xlsx_row_limit} = {parts}')

            # Download per parts
            for part in range(parts):
                logger.debug(f'Downloading part {part + 1}...')
                file_path_tmp = f'{dst_filename}_part{part + 1}'
                file_path_tmp_csv = f'{file_path_tmp}.csv'
                self.download_csv(f'SELECT * EXCEPT(_rn) FROM `{table_name_tmp}` WHERE _rn BETWEEN {(part * xlsx_row_limit) + 1} AND {(part + 1) * xlsx_row_limit}', f'{file_path_tmp}{os.sep}')
                csv_to_xlsx(file_path_tmp_csv, f'{file_path_tmp}.xlsx')
                os.remove(file_path_tmp_csv)
        except Exception as e:
            raise e
        finally:
            self.execute_query(f'DROP TABLE IF EXISTS `{table_name_tmp}`')

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
