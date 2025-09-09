from . import my_csv
from . import my_datetime
from . import my_env
from . import my_gcs
from . import my_queue
from . import my_string
from . import my_xlsx
from enum import StrEnum, Enum, auto
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from humanize import precisedelta, naturalsize
from loguru import logger
import csv
import datetime
import math
import os
import shutil
import textwrap
import time

PY_DATA_TYPE__BQ_DATA_TYPE = {
    int: "INTEGER",
    str: "STRING",
    float: "STRING",
}


class DataFileFormat(StrEnum):
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"


class DataFileCompression(StrEnum):
    GZIP = "GZIP"
    SNAPPY = "SNAPPY"


class LoadStrategy(Enum):
    OVERWRITE = auto()
    APPEND = auto()


class Dtype:
    INT64 = "INT64"
    INTEGER = "INTEGER"
    FLOAT64 = "FLOAT64"

    DECIMAL = "DECIMAL"

    STRING = "STRING"
    JSON = "JSON"

    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"

    BOOL = "BOOL"

    ARRAY_INT64 = "ARRAY<INT64>"
    ARRAY_INTEGER = "ARRAY<INTEGER>"
    ARRAY_FLOAT64 = "ARRAY<FLOAT64>"
    ARRAY_STRING = "ARRAY<STRING>"
    ARRAY_JSON = "ARRAY<JSON>"
    ARRAY_DATE = "ARRAY<DATE>"
    ARRAY_DATETIME = "ARRAY<DATETIME>"
    ARRAY_TIMESTAMP = "ARRAY<TIMESTAMP>"
    ARRAY_BOOL = "ARRAY<BOOL>"


class BQ:
    def __init__(self, location: str | None = None, project_id: str = None):
        if project_id is None and my_env.envs.GCP_PROJECT_ID is None:
            logger.warning("Using ADC for BigQuery authentication")

        # if location is None and my_env.envs.GCP_REGION is None:
        #     raise ValueError('GCP region must be set in environment variables.')

        self.client = bigquery.Client(
            project=project_id or my_env.envs.GCP_PROJECT_ID,
            location=location or my_env.envs.GCP_REGION,
        )
        logger.debug(f"BQ client open, project: {self.client.project}")

    # MARK: Query execution

    def execute_query(
        self,
        query: str | list[str],
        parameters: dict = {},
        dry_run: bool = False,
        temporary_table: bool = False,
    ) -> bigquery.QueryJob:
        # Reconstruct query, handle multiple queries in a single job
        is_multi = isinstance(query, list)
        queries = query if is_multi else [query]
        queries = [textwrap.dedent(q).strip() for q in queries]
        queries = [
            q if q.endswith(";") else q + ";" for q in queries
        ]  # Append ';' character for each query
        query = "\n".join(queries)

        # Evaluate parameter
        query_parameters = []
        for parameter, value in parameters.items():
            is_array = isinstance(value, list)
            value_type_py = type(value[0]) if is_array else type(value)
            if value_type_py not in PY_DATA_TYPE__BQ_DATA_TYPE:
                raise ValueError(
                    f"Unsupported type for parameter {parameter}: {value_type_py}. Supported types are: {list(PY_DATA_TYPE__BQ_DATA_TYPE.keys())}"
                )

            value_type_bq = PY_DATA_TYPE__BQ_DATA_TYPE[value_type_py]

            # Handle data type conversions
            if value_type_py == datetime.date:
                value = (
                    [v.strftime("%Y-%m-%d") for v in value]
                    if is_array
                    else value.strftime("%Y-%m-%d")
                )

            if is_array:
                query_parameters.append(
                    bigquery.ArrayQueryParameter(parameter, value_type_bq, value)
                )
            else:
                query_parameters.append(
                    bigquery.ScalarQueryParameter(parameter, value_type_bq, value)
                )

        logger.debug(f"🔎 Query:\n{query}")
        query_job_config = bigquery.QueryJobConfig(
            dry_run=dry_run, query_parameters=query_parameters
        )
        if temporary_table:
            query_job_config.destination = None
        t = time.time()
        query_job = self.client.query(query, job_config=query_job_config)
        (
            logger.info(
                f"Job tracking: https://console.cloud.google.com/bigquery?project={self.client.project}&j=bq:{self.client.location}:{query_job.job_id}&page=queryresults"
            )
            if not dry_run
            else None
        )
        query_job.result()  # Wait for the job to complete
        elapsed = precisedelta(datetime.timedelta(seconds=time.time() - t))

        if not is_multi:
            logger.info(
                f"[Job ID] {query_job.job_id}, [Processed] {naturalsize(query_job.total_bytes_processed)}, [Billed] {naturalsize(query_job.total_bytes_billed)}, [Affected] {query_job.num_dml_affected_rows or 0} row(s), [Elapsed] {elapsed}",
            )
        else:
            logger.info(f"[Job ID] {query_job.job_id} [Elapsed] {elapsed}")

            jobs: list[bigquery.QueryJob] = list(
                self.client.list_jobs(parent_job=query_job.job_id)
            )
            [
                logger.info(
                    f"[Script ID] {job.job_id}, [Processed] {naturalsize(job.total_bytes_processed)}, [Billed] {naturalsize(job.total_bytes_billed)}, [Affected] {job.num_dml_affected_rows or 0} row(s)",
                )
                for job in jobs
            ]

        return query_job

    # MARK: Table operations

    def create_table(
        self,
        dst_table_fqn: str,
        query: str,
        query_parameters: dict = {},
        *,
        description: str | None = None,
        schema: list[dict] | None = None,
        partition_by: str | None = None,
        clustering_fields: list[str] | None = None,
        expiration_timestamp_utc: datetime.datetime | None = None,
        require_partition_filter: bool = False,
        replace: bool = False,
    ):
        self.raise_for_invalid_table_fqn(dst_table_fqn)

        # Construct table options
        logger.debug("Constructing table options ...")
        table_options = []
        if expiration_timestamp_utc:
            table_options.append(
                f"  expiration_timestamp='{expiration_timestamp_utc.isoformat()}'"
            )
        if partition_by and require_partition_filter:
            table_options.append(f"  require_partition_filter=TRUE")
        if description:
            table_options.append(f"  description='{description}'")

        # Check if table exists
        logger.debug("Checking if destination table exists ...")
        dst_table_project_id, dst_table_dataset_id, dst_table_id = (
            self.get_table_fqn_parts(dst_table_fqn)
        )
        table_exist = self.is_table_exists(
            project_id=dst_table_project_id,
            dataset_id=dst_table_dataset_id,
            table_id=dst_table_id,
        )

        # Construct beautiful query string
        if table_exist and not replace:
            logger.debug("Table exists, constructing INSERT query ...")
            query_parts = [f"INSERT INTO `{dst_table_fqn}`"]
            if schema:
                schema_str = ",\n".join([column["name"] for column in schema])
                query_parts.append(f"(\n{schema_str}\n)")
            if table_options:
                table_options_str = ",\n".join(table_options)
                query_parts.append(f"OPTIONS (\n{table_options_str}\n)")
        else:
            logger.debug("Table not exist, constructing CREATE TABLE query ...")
            query_parts = [
                f"CREATE OR REPLACE TABLE `{dst_table_fqn}`",
            ]
            if schema:
                schema_str = ",\n".join(
                    [f'  {column["name"]} {column["data_type"]}' for column in schema]
                )
                query_parts.append(f"(\n{schema_str}\n)")
            if partition_by:
                query_parts.append(f"PARTITION BY {partition_by}")
            if clustering_fields:
                clustering_fields_str = ", ".join(
                    [f"`{field}`" for field in clustering_fields]
                )
                query_parts.append(f"CLUSTER BY {clustering_fields_str}")
            if table_options:
                table_options_str = ",\n".join(table_options)
                query_parts.append(f"OPTIONS (\n{table_options_str}\n)")
            query_parts.append("AS")
        query_parts.append(textwrap.dedent(query).strip())

        # Execute
        logger.debug("Executing query ...")
        query = "\n".join(query_parts)
        self.execute_query(query, parameters=query_parameters)

    def drop_table(self, bq_table_fqn: str):
        logger.info(f"Dropping table: {bq_table_fqn} ...")
        self.raise_for_invalid_table_fqn(bq_table_fqn)
        self.client.delete_table(bq_table_fqn, not_found_ok=True)

    # MARK: Table data

    def load_data(
        self,
        src_gcs_uri: str,
        dst_table_fqn: str,
        *,
        schema: list[dict] | None = None,
        partition_by: str | None = None,
        clustering_fields: list[str] | None = None,
        field_delimiter: str = ",",
        load_strategy: LoadStrategy = LoadStrategy.APPEND,
        format: DataFileFormat = DataFileFormat.CSV,
        compression=None,
    ):

        self.raise_for_invalid_table_fqn(dst_table_fqn)

        logger.debug(f"Loading CSV from {src_gcs_uri} into {dst_table_fqn} ...")

        # Construct LOAD options
        logger.debug("Constructing LOAD options ...")
        load_options = [  # https://cloud.google.com/bigquery/docs/reference/standard-sql/load-statements#load_option_list
            f"  format='{format}'",
            f"  uris=['{src_gcs_uri}']",
        ]
        if format == DataFileFormat.CSV:
            load_options.append(f"  skip_leading_rows=1")
            load_options.append(f"  field_delimiter='{field_delimiter}'")
            load_options.append(f"  allow_quoted_newlines=true")
        if compression:
            load_options.append(f"  compression='{compression}'")
        load_options_str = ",\n".join(load_options)

        # Construct beautiful query string
        logger.debug("Constructing LOAD query ...")
        schema_str = ",\n".join(
            [f'  {column["name"]} {column["data_type"]}' for column in schema]
        )
        query_parts = [
            f'LOAD DATA {"OVERWRITE" if load_strategy == LoadStrategy.OVERWRITE else "INTO"} `{dst_table_fqn}` (\n{schema_str}\n)'
        ]
        if partition_by:
            query_parts.append(f"PARTITION BY {partition_by}")
        if clustering_fields:
            clustering_fields_str = ", ".join(
                [f"`{field}`" for field in clustering_fields]
            )
            query_parts.append(f"CLUSTER BY {clustering_fields_str}")
        query_parts.append(f"FROM FILES (\n{load_options_str}\n)")
        query = "\n".join(query_parts)

        # Execute
        logger.debug("Executing query ...")
        self.execute_query(query)

    def export_data(
        self,
        query: str,
        dst_gcs_uri: str,
        *,
        parameters: dict = {},
        format: DataFileFormat = DataFileFormat.CSV,
        compression: DataFileCompression | None = None,
        header: bool = True,
        delimiter: str = ",",
    ):
        logger.debug(f"Exporting query into {dst_gcs_uri} ...")

        # GCS uri validation
        if (
            format == DataFileFormat.CSV
            and compression == DataFileCompression.GZIP
            and not dst_gcs_uri.endswith(".gz")
        ):
            raise ValueError(
                "GCS path need to ends with .gz if using compression = GCSCompression.GZIP"
            )
        elif (
            format == DataFileFormat.CSV
            and compression != DataFileCompression.GZIP
            and not dst_gcs_uri.endswith(".csv")
        ):
            raise ValueError(
                "GCS path need to ends with .csv if using format = GCSExportFormat.CSV"
            )
        elif format == DataFileFormat.PARQUET and not dst_gcs_uri.endswith(".parquet"):
            raise ValueError(
                "GCS path need to ends with .parquet if using format = GCSExportFormat.PARQUET"
            )

        # Construct options
        logger.debug("Constructing EXPORT options ...")
        options = [
            f"  uri='{dst_gcs_uri}'",
            f"  format='{format}'",
            f"  overwrite=TRUE",
        ]
        if format == DataFileFormat.CSV:
            options.append(
                f"  field_delimiter='{delimiter}'",
            )
            if header:
                options.append(
                    f'  header={"true" if header else "false"}',
                )
        if compression:
            options.append(f"  compression='{compression}'")
        options_str = ",\n".join(options)

        # Construct beautiful query string
        logger.debug("Constructing EXPORT query ...")
        query = (
            f"EXPORT DATA OPTIONS (\n"
            f"{options_str}\n"
            f")\n"
            f"AS (\n"
            f"{textwrap.dedent(query).strip()}\n"
            f");"
        )

        # Execute
        logger.debug("Executing query ...")
        self.execute_query(query=query, parameters=parameters)

    def upload_csv(
        self,
        src_filepath: str,
        dst_table_fqn: str,
        schema: list[dict] | None = None,
        gcs_bucket: str | None = None,
        partition_by: str = None,
        clustering_fields: list[str] = None,
        compression: DataFileCompression | None = None,
        load_strategy: LoadStrategy = LoadStrategy.APPEND,
    ):
        self.raise_for_invalid_table_fqn(dst_table_fqn)

        if compression == DataFileCompression.GZIP and not src_filepath.endswith(".gz"):
            raise ValueError(
                "Please provide file path with .gz extension if using compression = GZIP"
            )
        elif not src_filepath.endswith(".csv"):
            raise ValueError("Please provide file path with .csv extension")

        src_filename, src_fileextension = os.path.splitext(src_filepath)
        src_filename = os.path.basename(src_filename)  # Only get filename

        # # <<----- START: Upload to GCS

        # gcs = GCS(self.project_id)
        # tmp_dir = f'tmp/upload__{current_datetime_str()}'

        # # This will compress while splitting the compressed file to a certain bytes size because of GCS 4GB file limitation
        # # A single file can produce more than one compressed file in GCS
        # def producer(src_file: str):
        #     for dst_file in compress(src_file,
        # keep=True, max_size_bytes=ByteSize.GB * 3):
        #         yield (dst_file, )

        # def consumer(dst_file: str):
        #     remote_file_name = f'{tmp_dir}/{replace_nonnumeric(os.path.basename(dst_file), "_").lower()}.csv.gz'
        #     logger.debug(f'Uploading {dst_file} to {remote_file_name}...')
        #     blob = gcs.upload(dst_file, remote_file_name, move=True)
        #     return blob

        # blobs: list[storage.Blob]
        # _, blobs = ThreadingQ().add_producer(producer, src_filename).add_consumer(consumer).execute()

        # # END: Upload to GCS ----->>

        # Upload to GCS
        # TODO: Re-implement the producer-consumer model to upload multiple files
        gcs = my_gcs.GCS(bucket=gcs_bucket, project_id=self.client.project)
        dst_blobpath = f'tmp/my_bq/{my_datetime.get_current_datetime_str()}/{my_string.replace_nonnumeric(src_filename, "_").lower()}{src_fileextension}'
        gcs.upload(src_filepath, dst_blobpath)

        # Load to BQ
        try:
            self.load_data(
                f"gs://{gcs.bucket.name}/{dst_blobpath}",
                dst_table_fqn,
                schema=schema,
                partition_by=partition_by,
                clustering_fields=clustering_fields,
                format=DataFileFormat.CSV,
                compression=compression,
                load_strategy=load_strategy,
            )
        except:
            raise
        finally:
            gcs.delete_blob(dst_blobpath)

    def download_csv(
        self,
        query: str,
        dst_filepath: str,
        *,
        gcs_bucket: str | None = None,
        query_parameters: dict = {},
        csv_row_limit: int | None = None,
    ) -> str | list[str]:
        if not dst_filepath.endswith(".csv"):
            raise ValueError("Destination filename must ends with .csv")

        # Init
        gcs = my_gcs.GCS(bucket=gcs_bucket, project_id=self.client.project)

        # Generic function to export-download-combine csv file from BQ->GCS->local
        def _export_download_combine(
            query: str,
            dst_gcs_prefix: str,
            dst_filepath: str,
            query_parameters: dict = {},
        ):
            # Init tmp directory
            tmp_dirname = f"/tmp/my_bq_{my_datetime.get_current_datetime_str()}"
            if os.path.exists(tmp_dirname):
                shutil.rmtree(tmp_dirname, ignore_errors=True)
            os.makedirs(tmp_dirname, exist_ok=True)
            logger.debug(f"Temporary directory created: {tmp_dirname}")

            try:
                # Export to GCS
                dst_gcs_uri = f"gs://{gcs.bucket.name}/{dst_gcs_prefix}/*.csv.gz"
                self.export_data(
                    query,
                    dst_gcs_uri,
                    parameters=query_parameters,
                    format=DataFileFormat.CSV,
                    compression=DataFileCompression.GZIP,
                )

                # Download from GCS
                local_tmp_filepaths = []
                for tmp_blobs in gcs.list_blobs(dst_gcs_prefix):
                    local_tmp_filepath = os.path.join(
                        tmp_dirname, tmp_blobs.name.split("/")[-1]
                    )
                    gcs.download(tmp_blobs, local_tmp_filepath, move=True)
                    # logger.debug(f'Downloaded {tmp_blobs.name} to {local_tmp_filepath}')
                    local_tmp_filepaths.append(local_tmp_filepath)

                # Combine downloaded files
                my_csv.combine(
                    local_tmp_filepaths, dst_filepath, gzip=True, delete=True
                )
            except:
                raise
            finally:
                shutil.rmtree(tmp_dirname, ignore_errors=True)  # Remove local folder
                [
                    gcs.delete_blob(blob_filepath)
                    for blob_filepath in gcs.list_blobs(dst_gcs_prefix)
                ]  # Remove temporary GCS files

            logger.info(f"Export-download-combine done: {dst_filepath}")

        # Limited csv rows
        if csv_row_limit:
            tmp_table_fqn: str | None = None
            tmp_table_fqn_rn: str | None = None
            try:
                # Create temporary table
                query_job = self.execute_query(query, temporary_table=True)
                tmp_table_fqn = str(query_job.destination)
                logger.debug(f"Create temp table: {tmp_table_fqn}")

                # Create another temporary table for row numbering
                query_job = self.execute_query(
                    f"SELECT *, ROW_NUMBER() OVER() AS _rn FROM `{tmp_table_fqn}`",
                    temporary_table=True,
                )
                tmp_table_fqn_rn = str(query_job.destination)
                logger.debug(f"Create temp table (rn): {tmp_table_fqn_rn}")

                # Process parts
                count = list(
                    self.execute_query(
                        f"SELECT COUNT(1) FROM `{tmp_table_fqn_rn}`"
                    ).result()
                )[0][0]
                parts = math.ceil(count / csv_row_limit)
                logger.info(f"Total part: {count} / {csv_row_limit} = {parts}")
                dst_filepaths = []
                for part in range(parts):
                    dst_filepath_part = (
                        f'{dst_filepath.removesuffix(".csv")}_{part + 1:06}.csv'
                    )
                    _export_download_combine(
                        f"SELECT * EXCEPT(_rn) FROM `{tmp_table_fqn_rn}` WHERE _rn BETWEEN {(part * csv_row_limit) + 1} AND {(part + 1) * csv_row_limit} ORDER BY _rn",
                        dst_gcs_prefix=gcs.build_tmp_dirpath(),
                        dst_filepath=dst_filepath_part,
                    )
                    dst_filepaths.append(dst_filepath_part)
                return dst_filepaths
            except:
                raise
            finally:
                # Drop temporary tables
                if tmp_table_fqn_rn:
                    self.drop_table(tmp_table_fqn_rn)
                if tmp_table_fqn:
                    self.drop_table(tmp_table_fqn)

        # Unlimited csv rows
        else:
            _export_download_combine(
                query,
                gcs.build_tmp_dirpath(),
                dst_filepath,
                query_parameters=query_parameters,
            )
            return dst_filepath

        # query_job_result = query_job.result()
        # row_count = 0
        # file_index = 1

        # # Stream-download-split result
        # def open_file(f):
        #     if f:
        #         f.close()
        #     dst_filepath_part = f'{dst_filepath.removesuffix(".csv")}_{file_index:06}.csv' if row_limit else dst_filepath
        #     logger.info(f'Writing into file: {dst_filepath_part} ...')
        #     f = open(dst_filepath_part, 'w', newline='', encoding='utf-8')
        #     writer = csv.writer(f)
        #     writer.writerow([field.name for field in query_job_result.schema])  # Write header

        #     return f, writer

        # f, writer = open_file(None)
        # for row in query_job_result:
        #     writer.writerow(row)

        #     if row_limit:
        #         row_count += 1
        #         if row_count >= row_limit:
        #             row_count = 0
        #             file_index += 1
        #             f, writer = open_file(f)
        # if f:
        #     f.close()

    def download_xlsx(
        self, src_table_fqn: str, dst_filename: str, xlsx_row_limit: int = 950000
    ):
        if not dst_filename.endswith(".xlsx"):
            raise ValueError("Destination filename must ends with .xlsx!")

        # Create a temporary table acting as excel file splitting
        table_name_tmp = f"{src_table_fqn}_"
        self.execute_query(
            f"CREATE TABLE `{table_name_tmp}` AS SELECT *, ROW_NUMBER() OVER() AS _rn FROM `{src_table_fqn}`"
        )

        try:
            # Calculate the number of excel file parts based on row limit
            cnt = list(
                self.execute_query(
                    f"SELECT COUNT(1) AS cnt FROM `{src_table_fqn}`"
                ).result()
            )[0][0]
            parts = math.ceil(cnt / xlsx_row_limit)
            logger.debug(f"Total part: {cnt} / {xlsx_row_limit} = {parts}")

            # Download per parts
            for part in range(parts):
                logger.debug(f"Downloading part {part + 1}...")
                file_path_tmp = f"{dst_filename}_part{part + 1}"
                file_path_tmp_csv = f"{file_path_tmp}.csv"
                self.download_csv(
                    f"SELECT * EXCEPT(_rn) FROM `{table_name_tmp}` WHERE _rn BETWEEN {(part * xlsx_row_limit) + 1} AND {(part + 1) * xlsx_row_limit}",
                    f"{file_path_tmp}{os.sep}",
                )
                my_xlsx.csv_to_xlsx(file_path_tmp_csv, f"{file_path_tmp}.xlsx")
                os.remove(file_path_tmp_csv)
        except Exception as e:
            raise e
        finally:
            self.execute_query(f"DROP TABLE IF EXISTS `{table_name_tmp}`")

    # def copy_view(self, src_view_id: str, dst_view_id: str, drop: bool = False):
    #     src_project_id, src_dataset_id, _ = src_view_id.split('.')
    #     dst_project_id, dst_dataset_id, _ = dst_view_id.split('.')

    #     # Create or replace
    #     src_view = self.client.get_table(src_view_id)
    #     dst_view = bigquery.Table(dst_view_id)
    #     dst_view.view_query = src_view.view_query.replace(f'{src_project_id}.{src_dataset_id}', f'{dst_project_id}.{dst_dataset_id}')
    #     self.client.delete_table(dst_view, not_found_ok=True)
    #     self.client.create_table(dst_view)
    #     logger.debug(f'View {src_view_id} copied to {dst_view}')

    #     if drop:
    #         self.client.delete_table(src_view_id)
    #         logger.debug(f'View {src_view_id} dropped')

    # def copy_routine(self, src_routine_id: str, dst_routine_id: str, drop: bool = False):
    #     src_project_id, src_dataset_id, _ = src_routine_id.split('.')
    #     dst_project_id, dst_dataset_id, _ = dst_routine_id.split('.')

    #     # Create or replace
    #     src_routine = self.client.get_routine(src_routine_id)
    #     dst_routine = bigquery.Routine(dst_routine_id)
    #     dst_routine.body = src_routine.body.replace(f'{src_project_id}.{src_dataset_id}', f'{dst_project_id}.{dst_dataset_id}')
    #     dst_routine.type_ = src_routine.type_
    #     dst_routine.description = src_routine.description
    #     dst_routine.language = src_routine.language
    #     dst_routine.arguments = src_routine.arguments
    #     dst_routine.return_type = src_routine.return_type
    #     self.client.delete_routine(dst_routine, not_found_ok=True)
    #     self.client.create_routine(dst_routine)
    #     logger.debug(f'Routine {src_routine_id} copied to {dst_routine_id}')

    #     if drop:
    #         self.client.delete_routine(src_routine_id)
    #         logger.debug(f'Routine {src_routine_id} dropped')

    # MARK: Utilities

    @staticmethod
    def get_table_fqn_parts(name: str | list[str]) -> list[str] | list[list[str]]:
        """Get  fully qualified table name, following this format `<projectid>.<datasetid>.<tableid>`

        Args:
            name (str | list[str]): Input name (can be multiple)

        Returns:
            list[str] | list[list[str]]: The FQN parts. If the input is list then returns list of FQN parts instead.
        """

        if isinstance(name, list):
            return [BQ.get_table_fqn_parts(x) for x in name]

        split = name.split(".")
        if len(split) == 3:
            return split
        else:
            raise ValueError(f"{name} is not a valid table FQN")

    @staticmethod
    def raise_for_invalid_table_fqn(name: str | list[str]):
        """Raise an error if the provied name is a fully qualified table name

        Args:
            name (str | list[str]): Input name (can be multiple)

        Raises:
            ValueError: If name is not a fully qualified table name
        """

        if not BQ.get_table_fqn_parts(name):
            raise ValueError(f"{name} is not a valid table FQN")

    def is_table_exists(self, table_fqn: str) -> bool:
        self.raise_for_invalid_table_fqn(table_fqn)
        try:
            self.client.get_table(table_fqn)
            return True
        except NotFound:
            return False

    def close(self):
        self.client.close()
        logger.debug("BQ client close")
