import os
import shutil

from loguru import logger
from pydantic_settings import BaseSettings
from typing import Optional

from .my_input import ask_yes_no

ENV_DIR = os.path.expanduser(os.path.join('~', '.utill'))
ENV_FILE = os.path.join(ENV_DIR, 'env')

TEMPLATE_DIR = 'templates'
TEMPLATE_PG_FILENAME = os.path.join(os.path.dirname(__file__), TEMPLATE_DIR, 'pg.json')  # PostgreSQL connections
TEMPLATE_MB_FILENAME = os.path.join(os.path.dirname(__file__), TEMPLATE_DIR, 'mb.json')  # Metabase connections

PG_FILENAME = os.path.join(ENV_DIR, os.path.basename(TEMPLATE_PG_FILENAME))
MB_FILENAME = os.path.join(ENV_DIR, os.path.basename(TEMPLATE_MB_FILENAME))

# Make sure env dir always exists
if not os.path.exists(ENV_DIR):
    os.mkdir(ENV_DIR)


def init_pg_file():
    if os.path.exists(PG_FILENAME):
        if ask_yes_no(f'PostgreSQL connection file exists: {PG_FILENAME}, overwrite?'):
            shutil.copy(TEMPLATE_PG_FILENAME, PG_FILENAME)
            logger.warning(f'PostgreSQL connection file overwritten! {PG_FILENAME}')
        else:
            return

    shutil.copy(TEMPLATE_PG_FILENAME, PG_FILENAME)
    logger.info(f'PostgreSQL connection file created: {PG_FILENAME}')


def init_mb_file():
    if os.path.exists(MB_FILENAME):
        if ask_yes_no(f'Metabase connection file exists: {MB_FILENAME}, overwrite?'):
            shutil.copy(TEMPLATE_MB_FILENAME, MB_FILENAME)
            logger.warning(f'Metabase connection file overwritten! {MB_FILENAME}')
        else:
            return

    shutil.copy(TEMPLATE_MB_FILENAME, MB_FILENAME)
    logger.info(f'Metabase connection file created: {MB_FILENAME}')


class Envs(BaseSettings):

    GCP_PROJECT_ID: Optional[str] = None
    GCS_BUCKET: Optional[str] = None

    def set_var(self, k: str, v: str):
        setattr(self, k, v)

    def write(self):
        with open(ENV_FILE, 'w') as f:
            data = '\n'.join(['{}=\"{}\"'.format(k, str(getattr(self, k)).replace('\"', '\\\"')) for k in self.model_fields.keys()])
            f.write(data)

    class Config:
        env_file = ENV_FILE


envs = Envs()
