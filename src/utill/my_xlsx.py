import duckdb
import multiprocessing

from loguru import logger


def xlsx_to_csv(filename: str, sheet: str):
    con = duckdb.connect()
    return con.execute('install spatial;')\
        .execute('load spatial;')\
        .execute(f'select * from st_read(\'{filename}\', layer=\'{sheet}\');')\
        .fetchall()


def csv_to_xlsx(filename: str, output_file_path: str):
    logger.info(f'Converting csv \'{filename}\' into xlsx \'{output_file_path}\' ...')
    con = duckdb.connect()
    con.execute('install spatial;')\
        .execute('load spatial;')\
        .execute(f'set threads to {multiprocessing.cpu_count()};')\
        .execute(f'copy \'{filename}\' to \'{output_file_path}\' with(format gdal, driver \'xlsx\')')
