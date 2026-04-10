import os
import shutil

from ._lazy_logger import logger


ENV_DIR = os.path.expanduser(os.path.join("~", ".utill"))
ENV_FILE = os.path.join(ENV_DIR, "env")

TEMPLATE_DIR = "templates"
TEMPLATE_PG_FILENAME = os.path.join(
    os.path.dirname(__file__), TEMPLATE_DIR, "pg.json"
)  # PostgreSQL connections
TEMPLATE_MB_FILENAME = os.path.join(
    os.path.dirname(__file__), TEMPLATE_DIR, "mb.json"
)  # Metabase connections

PG_FILENAME = os.path.join(ENV_DIR, os.path.basename(TEMPLATE_PG_FILENAME))
MB_FILENAME = os.path.join(ENV_DIR, os.path.basename(TEMPLATE_MB_FILENAME))


def _ensure_env_dir_exists() -> None:
    if not os.path.exists(ENV_DIR):
        os.mkdir(ENV_DIR)


def _parse_env_file(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        return {}

    parsed: dict[str, str] = {}
    with open(path, "r") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            parsed[key] = value

    return parsed


def init_pg_file():
    from .input import ask_yes_no

    _ensure_env_dir_exists()

    if os.path.exists(PG_FILENAME):
        if ask_yes_no(f"PostgreSQL connection file exists: {PG_FILENAME}, overwrite?"):
            shutil.copy(TEMPLATE_PG_FILENAME, PG_FILENAME)
            logger.warning(f"PostgreSQL connection file overwritten! {PG_FILENAME}")
        else:
            return

    shutil.copy(TEMPLATE_PG_FILENAME, PG_FILENAME)
    logger.info(f"PostgreSQL connection file created: {PG_FILENAME}")


def init_mb_file():
    from .input import ask_yes_no

    _ensure_env_dir_exists()

    if os.path.exists(MB_FILENAME):
        if ask_yes_no(f"Metabase connection file exists: {MB_FILENAME}, overwrite?"):
            shutil.copy(TEMPLATE_MB_FILENAME, MB_FILENAME)
            logger.warning(f"Metabase connection file overwritten! {MB_FILENAME}")
        else:
            return

    shutil.copy(TEMPLATE_MB_FILENAME, MB_FILENAME)
    logger.info(f"Metabase connection file created: {MB_FILENAME}")


class Envs:
    model_fields = {
        "GCP_PROJECT_ID": None,
        "GCP_REGION": None,
        "GCS_BUCKET": None,
    }

    def __init__(self):
        for key in self.model_fields:
            setattr(self, key, None)
        self.reload()

    def reload(self):
        _ensure_env_dir_exists()
        file_values = _parse_env_file(ENV_FILE)
        for key in self.model_fields:
            value = os.getenv(key, file_values.get(key))
            setattr(self, key, value)

    def set_var(self, k: str, v: str):
        if k not in self.model_fields:
            raise ValueError(f"Unknown environment variable: {k}")
        setattr(self, k, v)

    def write(self):
        _ensure_env_dir_exists()
        with open(ENV_FILE, "w") as f:
            data = "\n".join(
                [
                    '{}="{}"'.format(k, str(getattr(self, k)).replace('"', '\\"'))
                    for k in self.model_fields.keys()
                ]
            )
            f.write(data)


_ensure_env_dir_exists()
envs = Envs()
