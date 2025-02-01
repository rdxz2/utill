import gzip
import os
import shutil

from loguru import logger


def compress(src_file: str, keep: bool = False):
    src_file = os.path.expanduser(src_file)
    dst_file = src_file + '.gz'

    os.remove(dst_file) if os.path.exists(dst_file) else None
    logger.debug(f'📄 Compress {dst_file} --> {dst_file}')
    with open(src_file, 'rb') as f_in:
        with gzip.open(dst_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(src_file) if not keep else None

    return dst_file


def decompress(src_file: str, keep: bool = False):
    if not src_file.endswith('.gz'):
        raise ValueError('File name not ends with .gz!')

    src_file = os.path.expanduser(src_file)
    dst_file = src_file.removesuffix('.gz')

    os.remove(dst_file) if os.path.exists(dst_file) else None
    logger.debug(f'📄 Decompress {src_file} --> {dst_file}')
    with gzip.open(src_file, 'rb') as f_in:
        with open(dst_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    keep or os.remove(src_file)

    return dst_file


def make_sure_directory_exists(dirname: str):
    if not os.path.exists(os.path.dirname(os.path.expanduser(dirname))):
        os.makedirs(os.path.dirname(os.path.expanduser(dirname)))


def make_sure_path_is_directory(path: str):
    if not path.endswith(os.sep):
        raise ValueError(f'Please specify directory name ending with \'{os.sep}\' character, example for Linux: \'/home/my_username/Downloads/my_folder/\'!')


def read_last_line(filename: str) -> str:
    filename = os.path.expanduser(filename)
    with open(filename, 'rb') as f:
        try:  # Catch OSError in case of a one line file
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        return f.readline().decode()
