def _init(mode: str):
    import logging

    from ..settings import envs

    logger = logging.getLogger("utill")
    from ..settings import init_mb_file
    from ..settings import init_pg_file

    match mode:
        case "google-cloud":
            setattr(envs, "GCP_PROJECT_ID", input("GCP_PROJECT_ID: "))
            setattr(envs, "GCP_REGION", input("GCP_REGION: "))
            setattr(envs, "GCS_BUCKET", input("GCS_BUCKET: "))
            envs.write()
            logger.info("Google cloud configuration initialized")
        case "postgresql":
            init_pg_file()
        case "metabase":
            init_mb_file()
        case _:
            logger.warning(f"Mode '{mode}' not recognized")


def _list(module: str = None):
    import json
    import logging
    import os

    from ..settings import MB_FILENAME

    logger = logging.getLogger("utill")
    from ..settings import PG_FILENAME
    from ..settings import envs
    from ..string import mask

    match module:
        case "postgresql":
            if not os.path.exists(PG_FILENAME):
                logger.error("PostgreSQL configuraiton not exists")
                return

            config: dict = json.loads(open(PG_FILENAME, "r").read())
            for k, v in config.items():
                print(k)
                for k2, v2 in v.items():
                    print(f"\t{k2} = {mask(str(v2)) if k2 in ('password',) else v2}")

        case "metabase":
            if not os.path.exists(MB_FILENAME):
                logger.error("Metabase configuration not exists")
                return

            config: dict = json.loads(open(MB_FILENAME, "r").read())
            for k, v in config.items():
                print(f"{k} = {mask(str(v)) if k in ('api_key',) else v}")
        case _:
            for env in envs.model_fields:
                print(f"{env} = {getattr(envs, env)}")


def _set(vars: list[tuple[str, str]]):
    from ..settings import envs

    for k, v in vars:
        setattr(envs, k, v)

    envs.write()
