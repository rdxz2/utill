from .my_datetime import get_current_datetime_str
from .my_env import envs
from google.cloud import storage
from loguru import logger
import os


class GCS:

    def __init__(self, bucket: str | None = None, project_id: str | None = None):
        if project_id is None and envs.GCP_PROJECT_ID is None:
            logger.warning("Using ADC for GCS authentication")

        if bucket is None and envs.GCS_BUCKET is None:
            raise ValueError(
                "Bucket name must be provided either as an argument or set in environment variables."
            )

        self.client = storage.Client(project=project_id or envs.GCP_PROJECT_ID)
        self.bucket = self.client.bucket(bucket or envs.GCS_BUCKET)
        logger.debug(
            f"GCS client open, project: {self.client.project}, bucket: {self.bucket.name}"
        )

    def get_blob(self, blobpath: str) -> storage.Blob:
        return self.bucket.blob(blobpath)

    def list_blobs(self, prefix: str) -> list[storage.Blob]:
        return self.bucket.list_blobs(prefix=prefix)

    def delete_blob(self, blobpath: str | storage.Blob) -> storage.Blob:
        blob = self.get_blob(blobpath) if isinstance(blobpath, str) else blobpath
        return blob.delete()

    def copy(
        self,
        src_blobpath: str,
        dst_blobpath: str,
        dst_bucket: str = None,
        move: bool = False,
    ):
        src_bucket = self.bucket
        src_blob = self.get_blob(src_blobpath)
        dst_bucket = dst_bucket or src_bucket.name

        self.bucket.copy_blob(src_blob, dst_bucket, dst_blobpath)

        # Move mode
        if move:
            self.delete_blob(src_blobpath)
            logger.debug(
                f"Moved gs://{src_bucket}/{src_blobpath} to gs://{dst_bucket}/{dst_blobpath}"
            )
        # Copy mode
        else:
            logger.debug(
                f"Copied gs://{src_bucket}/{src_blobpath} to gs://{dst_bucket}/{dst_blobpath}"
            )

    def upload(self, src_filepath: str, dst_blobpath: str, move: bool = False):
        blob = self.get_blob(dst_blobpath)
        blob.upload_from_filename(src_filepath)

        # Move mode
        if move:
            os.remove(src_filepath)
            logger.debug(f"Moved {src_filepath} to gs://{self.bucket.name}/{blob.name}")
        # Copy mode
        else:
            logger.debug(
                f"Uploaded {src_filepath} to gs://{self.bucket.name}/{blob.name}"
            )

    def download(
        self, src_blobpath: str | storage.Blob, dst_filepath: str, move: bool = False
    ):
        blob = (
            self.get_blob(src_blobpath)
            if isinstance(src_blobpath, str)
            else src_blobpath
        )
        blob.download_to_filename(dst_filepath)

        if move:
            self.delete_blob(blob)
            logger.debug(f"Moved gs://{self.bucket.name}/{blob.name} to {dst_filepath}")
        else:
            logger.debug(
                f"Copied gs://{self.bucket.name}/{blob.name} to {dst_filepath}"
            )

    # MARK: Utilities

    @staticmethod
    def build_tmp_dirpath(prefix: str = "tmp") -> str:
        """
        Builds a temporary directory path in the GCS bucket.
        """
        return f"{prefix}/{get_current_datetime_str()}"

    def close(self):
        self.client.close()
        logger.debug("GCS client closed")
