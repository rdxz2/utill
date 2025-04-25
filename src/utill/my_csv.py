import csv
import gzip
import os
import sys

from loguru import logger

from .my_const import ByteSize
from .my_file import decompress


def read_header(filename: str):
    filename = os.path.expanduser(filename)
    with open(filename, 'r') as f:
        csvreader = csv.reader(f)
        return next(csvreader)


def write(filename: str, rows: list[tuple], append: bool = False):
    filename = os.path.expanduser(filename)
    with open(filename, 'a' if append else 'w') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(rows)


def compress(src_filename: str, keep: bool = False, max_size_bytes=ByteSize.GB, src_fopen=None, header=None, file_count=1):
    src_filename = os.path.expanduser(src_filename)
    current_size = 0
    dst_filename = f'{src_filename}_part{str(file_count).rjust(6, "0")}.gz'
    os.remove(dst_filename) if os.path.exists(dst_filename) else None
    logger.debug(f'📄 Compress csv {src_filename} --> {dst_filename}')
    gz = gzip.open(dst_filename, 'wt')

    src_fopen = src_fopen or open(src_filename)
    header = header or src_fopen.readline()

    gz.write(header)

    while True:
        line = src_fopen.readline()
        if not line:
            break

        gz.write(line)
        current_size += len(line.encode('utf-8'))

        if current_size >= max_size_bytes:
            gz.close()
            yield dst_filename

            file_count += 1
            yield from compress(src_filename, keep, max_size_bytes, src_fopen, header, file_count)
            return

    gz.close()
    os.remove(src_filename) if not keep else None
    yield dst_filename


def combine(src_filenames: list[str], dst_filename: str) -> None:
    csv.field_size_limit(min(sys.maxsize, 2147483646))  # FIX: _csv.Error: field larger than field limit (131072)

    if not dst_filename.endswith('.csv'):
        raise ValueError('Output filename must ends with \'.csv\'!')

    first_file = True
    with open(dst_filename, 'w') as fout:
        csvwriter = csv.writer(fout)

        for src_filename in src_filenames:
            src_filename = os.path.expanduser(src_filename)

            # Decompress gzipped csv
            if src_filename.endswith('.csv.gz'):
                src_filename = decompress(src_filename)

            # Copy
            with open(src_filename, 'r') as fin:
                csvreader = csv.reader(fin)

                # Copy the header if this is the first file
                if first_file:
                    csvwriter.writerow(next(csvreader))
                    first_file = False
                # Else, skip the header
                else:
                    next(csvreader)

                [csvwriter.writerow(row) for row in csvreader]

            logger.info(f'✅ Combine {src_filename}')
