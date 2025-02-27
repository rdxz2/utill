import csv
import json
import os
import psycopg
import psycopg.rows

from loguru import logger
from textwrap import dedent

from .my_env import PG_FILENAME
from .my_string import generate_random_string
from .my_tunnel import establish_tunnel


class PG:
    def __init__(
        self,
        connection=None,
        config_source: str | dict = PG_FILENAME,
        autocommit: bool = True,
    ) -> None:
        if type(config_source) == str:
            if connection is None:
                raise ValueError('Connection name must be provided when using file source!')
            conf = json.loads(open(os.path.expanduser(config_source)).read())[connection]
        elif type(config_source) == dict:
            conf = config_source
        (_, host, port) = establish_tunnel(conf)
        self.db_host = host
        self.db_port = port
        self.db_username = conf['username']
        self.db_password = conf['password']
        self.db_name = conf['db']
        self.conf = conf

        self.conn = None
        self.cursor = None
        self.establish_connection(autocommit)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

    def establish_connection(self, autocommit: bool, row_factory: psycopg.rows = psycopg.rows.dict_row):
        self.conn = psycopg.connect(f'postgresql://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}', autocommit=autocommit)
        self.cursor = self.conn.cursor(row_factory=row_factory)
        logger.debug(f'PG client open: {self.db_username}@{self.db_host}:{self.db_port}/{self.db_name}, autocommit={self.conn.autocommit}')

    def change_autocommit(self, autocommit: bool):
        if autocommit == self.conn.autocommit:
            return

        self.conn.autocommit = autocommit

    def execute_query(self, query: str, *params):
        # Make sure connection alive
        if self.conn.closed:
            self.establish_connection(self.conn.autocommit)

        query = query.strip()
        logger.debug(f'🔎 Query:\n{query}')

        return self.cursor.execute(query, params)

    def download_csv(self, query: str, file_path: str) -> None:
        query = dedent(
            f'''
            COPY ({query})
            TO STDOUT
            WITH DELIMITER ','
            CSV HEADER;
            '''
        )
        logger.debug(f'🔎 Query:\n{query}')
        with open(os.path.expanduser(file_path), 'wb') as f:
            with self.cursor.copy(query) as copy:
                for data in copy:
                    f.write(data)

    def pg_to_pg(self, pg: "PG", source_table: str, target_table: str, cols: list[str] = None) -> None:
        tmp_filename = generate_random_string() + '.csv'
        cols_str = ','.join([f'"{x}"' for x in cols]) if (cols is not None and cols != []) else '*'
        try:
            self.download_csv(f'SELECT {cols_str} FROM {source_table}', tmp_filename)
            pg.upload_csv(tmp_filename, target_table)
        except:
            raise
        finally:
            os.remove(tmp_filename) if os.path.exists(tmp_filename) else None

    def check_table_existence(self, table_name: str) -> bool:
        if not self.execute_query('''SELECT count(1) AS "cnt" FROM "information_schema"."tables" WHERE "table_schema" || '.' || "table_name" = '%s';''', table_name).fetchone()['cnt']:
            raise Exception(f'Target table \'{table_name}\' not created, please create it first!')

    def upload_tuples(self, cols: list[str], tuples: list[tuple], table_name: str) -> None:
        self.check_table_existence(table_name)

        cols_str = ','.join([f'"{x}"' for x in cols])
        query = f'''COPY {table_name}({cols_str}) FROM STDIN'''
        logger.debug(f'🔎 Query:\n{query}')
        with self.cursor.copy(query) as copy:
            for row in tuples:
                copy.write_row(row)

    def upload_list_of_dict(self, data: list[dict], table_name: str) -> None:
        self.check_table_existence(table_name)

        if len(data) == 0:
            raise ValueError('No data to upload!')

        cols = data[0].keys()
        cols_str = ','.join([f'"{x}"' for x in cols])
        query = f'''COPY {table_name}({cols_str}) FROM STDIN'''
        logger.debug(f'🔎 Query:\n{query}')
        with self.cursor.copy(query) as copy:
            for row in data:
                copy.write_row(tuple(row[col] for col in cols))

    def upload_csv(self, file_path: str, table_name: str) -> None:
        self.check_table_existence(table_name)

        cols_str = ','.join([f'"{x}"' for x in next(csv.reader(open(file_path, 'r')))])
        query = dedent(
            f'''
            COPY {table_name}({cols_str})
            FROM STDIN
            DELIMITER ','
            CSV HEADER;
            '''
        )
        logger.debug(f'🔎 Query:\n{query}')
        with open(os.path.expanduser(file_path), 'r') as f:
            with self.cursor.copy(query) as copy:
                while data := f.read(1024):
                    copy.write(data)

    def create_index(self, table_name: str, index: str | list[str], unique: bool = False) -> None:
        try:
            index = index if type(index) == list else [index]
            indexes = ','.join([f'"{x}"' for x in index])
            self.execute_query(f'CREATE {"UNIQUE " if unique else ""}INDEX ON "{table_name}" ({indexes});', return_df=False)
        except Exception as e:
            self.rollback()
            raise e

    def rollback(self):
        self.conn.rollback()
        logger.debug('🚫 Transaction rollback')

    def commit(self):
        self.conn.commit()
        logger.debug('✅ Transaction commit')

    def close(self):
        self.cursor.close()
        self.conn.close()
        logger.debug('PG client close')
