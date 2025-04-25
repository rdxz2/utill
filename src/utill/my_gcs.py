import os
import re

from google.cloud import storage
from loguru import logger

from .my_env import envs


class GCS:

    def __init__(self, project: str = None, bucket_name: str = None):
        self.project = project if project is not None else envs.GCP_PROJECT_ID
        self.client = storage.Client(project=self.project)

        bucket_name_parts = (bucket_name or envs.GCS_BUCKET).split('/')
        self.change_bucket(bucket_name_parts[0])
        self.base_path = '/'.join(bucket_name_parts[1:]) if len(bucket_name_parts) > 1 else None
        not self.base_path or logger.debug(f'Base path: {self.base_path}')

        logger.debug(f'GCS client open, project: {project or "<application-default>"}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close_client()

    def _construct_path(self, path: str) -> str:
        return f'{self.base_path}/{path}' if self.base_path else path

    def change_bucket(self, bucket_name: str):
        if not bucket_name:
            raise ValueError('Bucket name needed')
        self.bucket = self.client.bucket(bucket_name)
        logger.debug(f'Change bucket to {self.bucket.name}')

    def get(self, path: str) -> storage.Blob:
        path = self._construct_path(path)
        return self.bucket.blob(path)

    def list(self, path: str) -> list[storage.Blob]:
        path = self._construct_path(path)
        if '*' in path:
            path_prefix = path.split('*')[0]
            regex_pattern = '^' + re.escape(path).replace('\\*', '.*') + '$'
            regex = re.compile(regex_pattern)
            return [x for x in self.bucket.list_blobs(prefix=path_prefix) if regex.match(x.name)]

        return list(self.bucket.list_blobs(prefix=path))

    def copy(self, src_path: str, dst_path: str, mv: bool = False):
        src_blob = self.get(src_path)
        dst_blob = self.get(dst_path)

        dst_blob.rewrite(src_blob)

        logger.debug(f'✅ Copy gs://{src_blob.bucket.name}/{src_blob.name} to gs://{dst_blob.bucket.name}/{dst_blob.name}')

        not mv or GCS.remove_blob(src_blob)

        return dst_blob

    def copy_to_other_gcs(self, src_blob: storage.Blob, dst_gcs: "GCS", dst_path: str, mv: bool = False):
        self.bucket.copy_blob(src_blob, dst_gcs.bucket, dst_path)
        dst_blob = dst_gcs.get(dst_path)

        not mv or GCS.remove_blob(src_blob)

        return dst_blob

    def upload(self, local_path: str, remote_path: str, mv: bool = False):
        local_path = os.path.expanduser(local_path)

        if not os.path.exists(local_path):
            raise FileNotFoundError(f'File not found: {local_path}')

        blob = self.get(remote_path)
        blob.upload_from_filename(local_path)

        logger.debug(f'✅ Upload {local_path} to gs://{self.bucket.name}/{blob.name}')

        not mv or os.remove(local_path)

        return blob

    def download(self, obj: str | storage.Blob, local_path: str, mv: bool = False):
        local_path = os.path.expanduser(local_path)
        is_blob = type(obj) == storage.Blob

        if os.path.isdir(local_path):
            local_path = os.path.join(local_path, obj.name.split('/')[-1] if is_blob else os.path.basename(obj))
            if not os.path.dirname(local_path):
                raise FileNotFoundError(f'Destination directory not found: {os.path.dirname(local_path)}')

        blob = obj if is_blob else self.get(obj)
        blob.download_to_filename(local_path)

        logger.debug(f'✅ Download gs://{self.bucket.name}/{blob.name} to {local_path}')

        not mv or GCS.remove_blob(blob)

        return blob

    def remove(self, remote_path: str):
        blob = self.get(remote_path)

        GCS.remove_blob(blob)

        return blob

    def close_client(self):
        self.client.close()
        logger.debug('GCS client close')

    @staticmethod
    def remove_blob(blob: storage.Blob):
        blob.delete()
        logger.debug(f'🗑️ Remove gs://{blob.bucket.name}/{blob.name}')
