import os
import shutil

from loguru import logger
from pydantic_settings import BaseSettings
from typing import Optional

from .my_input import ask_yes_no

ENV_DIR = os.path.expanduser(os.path.join('~', '.utill'))
ENV_FILE = os.path.join(ENV_DIR, 'env')

TEMPLATE_DIR = 'templates'
TEMPLATE_DB_FILENAME = os.path.join(os.path.dirname(__file__), TEMPLATE_DIR, 'db.json')  # Database connections
TEMPLATE_MB_FILENAME = os.path.join(os.path.dirname(__file__), TEMPLATE_DIR, 'mb.json')  # Metabase connections

DB_FILENAME = os.path.join(ENV_DIR, os.path.basename(TEMPLATE_DB_FILENAME))
MB_FILENAME = os.path.join(ENV_DIR, 'mb.json')

# Make sure env dir always exists
if not os.path.exists(ENV_DIR):
    os.mkdir(ENV_DIR)


def init_db_file():
    if os.path.exists(DB_FILENAME):
        if ask_yes_no(f'DB file exists: {DB_FILENAME}, overwrite?'):
            shutil.copy(TEMPLATE_DB_FILENAME, DB_FILENAME)
            logger.warning(f'DB file overwritten! {DB_FILENAME}')
        else:
            return

    shutil.copy(TEMPLATE_DB_FILENAME, DB_FILENAME)
    logger.info(f'DB file created: {DB_FILENAME}')


def init_mb_file():
    if os.path.exists(MB_FILENAME):
        if ask_yes_no(f'MB file exists: {MB_FILENAME}, overwrite?'):
            shutil.copy(TEMPLATE_MB_FILENAME, MB_FILENAME)
            logger.warning(f'MB file overwritten! {MB_FILENAME}')
        else:
            return

    shutil.copy(TEMPLATE_MB_FILENAME, MB_FILENAME)
    logger.info(f'MB file created: {MB_FILENAME}')


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
