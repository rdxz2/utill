def _encrypt(src: str, password: str, output: str = None, force: bool = False):
    from pathlib import Path

    # Get the password string from file, if exists
    path_password = Path(password).expanduser()
    if path_password.exists():
        if not path_password.is_file():
            raise ValueError(f'Password path is not a file: {password}')
        else:
            password = open(path_password.as_posix(), 'r').read().strip()

    path_src = Path(src).expanduser()
    if path_src.exists():
        if path_src.is_dir():
            raise ValueError(f'Source file is a directory: {src}')

        # Do encryption
        from ..my_encryption import encrypt_file
        if output:
            encrypt_file(path_src.as_posix(), password, dst_filename=output, overwrite=force)
        else:
            print(encrypt_file(path_src.as_posix(), password))
    else:
        # Do encryption
        from ..my_encryption import encrypt_string
        print(encrypt_string(src, password))
