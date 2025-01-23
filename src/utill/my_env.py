import os
import shutil

from pydantic_settings import BaseSettings
from typing import Optional

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
    shutil.copy(TEMPLATE_DB_FILENAME, DB_FILENAME)


def init_mb_file():
    shutil.copy(TEMPLATE_MB_FILENAME, MB_FILENAME)


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
