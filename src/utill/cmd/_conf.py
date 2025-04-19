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
        case 'metabase':
            init_mb_file()
        case _:
            logger.warning(f'Mode \'{mode}\' not recognized')


def _list(module: str = None):
    import json
    import os

    from loguru import logger

    from ..my_env import envs, PG_FILENAME, MB_FILENAME
    from ..my_string import mask

    match module:
        case 'postgresql':
            if not os.path.exists(PG_FILENAME):
                logger.error('PostgreSQL configuraiton not exists')
                return

            config: dict = json.loads(open(PG_FILENAME, 'r').read())
            for k, v in config.items():
                print(k)
                for k2, v2 in v.items():
                    print(f'\t{k2} = {mask(str(v2)) if k2 in ("password", ) else v2}')

        case 'metabase':
            if not os.path.exists(MB_FILENAME):
                logger.error('Metabase configuration not exists')
                return

            config: dict = json.loads(open(MB_FILENAME, 'r').read())
            for k, v in config.items():
                print(f'{k} = {mask(str(v)) if k in ("api_key", ) else v}')
        case _:
            for env in envs.model_fields:
                print(f'{env} = {getattr(envs, env)}')


def _set(vars: list[tuple[str, str]]):
    from ..my_env import envs

    for k, v in vars:
        setattr(envs, k, v)

    envs.write()
