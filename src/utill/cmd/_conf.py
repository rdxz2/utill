def _init(mode: str):
    from loguru import logger

    from ..my_env import envs, init_pg_file, init_mb_file

    match mode:
        case 'google-cloud':
            setattr(envs, 'GCP_PROJECT_ID', input('GCP_PROJECT_ID: '))
            setattr(envs, 'GCS_BUCKET', input('GCS_BUCKET: '))
            envs.write()
            logger.info('Google cloud configuration initialized')
        case 'postgresql':
            init_pg_file()
            logger.info('PostgreSQL connection file initialized')
        case 'metabase':
            init_mb_file()
            logger.info('Metabase connection file initialized')
        case _:
            logger.warning(f'Mode \'{mode}\' not recognized')


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
