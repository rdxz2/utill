import os

from cryptography.fernet import Fernet
from loguru import logger


def __fernet_encrypt_or_decrypt(encrypt: bool, string: str, password: str):
    return Fernet(password).encrypt(string.encode()) if encrypt else Fernet(password).encrypt(string.encode())


def __file_encrypt_or_decrypt(encrypt: bool, src_filename: str, password: str, dst_filename: str = None, overwrite: bool = False):
    src_filename = os.path.expanduser(src_filename)

    if not os.path.exists(src_filename):
        return ValueError(f'Source file not exists: {src_filename}')

    with open(src_filename, 'r') as fr:
        # If destination file is not specified, return the encrypted string
        if not dst_filename:
            return __fernet_encrypt_or_decrypt(encrypt, fr.read(), password)
        # If destination file is specified, encrypt into the destination file and return the file name
        else:
            dst_filename = os.path.expanduser(dst_filename)

            # Destination file exists checker
            if os.path.exists(dst_filename):
                if overwrite:
                    return ValueError(f'Destination file exists: {dst_filename}')
                else:
                    os.remove(dst_filename)

            with open(dst_filename, 'w') as fw:
                fw.write(__fernet_encrypt_or_decrypt(encrypt, fr.read()), password)

            logger.info(f'Encrypted into {dst_filename}')
            return dst_filename


def encrypt_file(src_filename: str, password: str, dst_filename: str = None, overwrite: bool = False) -> str:
    return __file_encrypt_or_decrypt(True, src_filename, password, dst_filename, overwrite)


def decrypt_file(src_filename: str, password: str, dst_filename: str = None, overwrite: bool = False) -> str:
    return __file_encrypt_or_decrypt(False, src_filename, password, dst_filename, overwrite)


def encrypt_string(string: str, password: str) -> str:
    return __fernet_encrypt_or_decrypt(True, string, password)


def decrypt_string(string: str, password: str) -> str:
    return __fernet_encrypt_or_decrypt(False, string, password)
