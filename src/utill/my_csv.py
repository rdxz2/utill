import csv
import gzip
import os
import sys

from loguru import logger

from .my_file import decompress, adjust_sep


def read_header(filename: str):
    with open(filename, 'r') as f:
        csvreader = csv.reader(f)
        return next(csvreader)


def append(filename: str, rows: list[tuple]):
    with open(filename, 'a') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(rows)


def compress(src_file: str, keep: bool = False, max_size_bytes=1024 ** 3 * 1, src_fopen=None, header=None, file_count=1):
    src_file = adjust_sep(os.path.expanduser(src_file))
    current_size = 0
    dst_file = f'{src_file}_part{str(file_count).rjust(6, "0")}.gz'
    os.remove(dst_file) if os.path.exists(dst_file) else None
    logger.debug(f'📄 Compress csv {src_file} --> {dst_file}')
    gz = gzip.open(dst_file, 'wt')

    src_fopen = src_fopen or open(src_file)
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
            yield dst_file

            file_count += 1
            yield from compress(src_file, keep, max_size_bytes, src_fopen, header, file_count)
            return

    gz.close()
    os.remove(src_file) if not keep else None
    yield dst_file


def combine(input_filenames: list[str], output_filename: str) -> None:
    csv.field_size_limit(min(sys.maxsize, 2147483646))  # FIX: _csv.Error: field larger than field limit (131072)

    if not output_filename.endswith('.csv'):
        raise ValueError('Output filename must ends with \'.csv\'!')

    first_file = True
    with open(output_filename, 'w') as fout:
        csvwriter = csv.writer(fout)

        for input_filename in input_filenames:
            # Decompress gzipped csv
            if input_filename.endswith('.csv.gz'):
                input_filename = decompress(input_filename)

            # Copy
            with open(input_filename, 'r') as fin:
                csvreader = csv.reader(fin)

                # Copy the header if this is the first file
                if first_file:
                    csvwriter.writerow(next(csvreader))
                # Else, skip the header
                else:
                    next(csvreader)

                [csvwriter.writerow(row) for row in csvreader]

            logger.info(f'✅ Combine {input_filename}')
