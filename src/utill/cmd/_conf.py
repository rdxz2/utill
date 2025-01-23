def _init():
    from ..my_env import init_db_file, init_mb_file

    init_db_file()
    init_mb_file()


def _list():
    from loguru import logger

    from ..my_env import envs

    for env in envs.model_fields:
        logger.info(f'{env} = {getattr(envs, env)}')


def _set(vars: list[tuple[str, str]]):
    from ..my_env import envs

    for k, v in vars:
        setattr(envs, k, v)

    envs.write()
