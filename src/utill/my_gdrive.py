from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from humanize import naturalsize
import enum
import logging
import os


log = logging.getLogger(__name__)


class Role(enum.StrEnum):
    READER = "reader"
    WRITER = "writer"
    COMMENTER = "commenter"
    OWNER = "owner"


class GDrive:
    """
    Custom hook for Google Drive integration in Airflow.
    This hook can be used to interact with Google Drive APIs.
    """

    def __init__(self):
        credentials, project = default(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.file",
            ]
        )
        drive_service = build("drive", "v3", credentials=credentials)
        self.connection = drive_service

    # region Folder operations

    def get_folder_by_name(self, *, parent_folder_id: str, name: str) -> str | None:
        """
        Retrieves a folder by its name within a specified Google Drive folder.
        :param folder_id: The ID of the parent folder to search in.
        :param name: The name of the folder to find.
        :return: The ID of the found folder or None if not found.
        """
        query = f"'{parent_folder_id}' in parents and name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = (
            self.connection.files()
            .list(q=query, fields="files(id)", supportsAllDrives=True)
            .execute()
        )
        items = results.get("files", [])

        return items[0]["id"] if items else None

    def create_folder(
        self, folder_name: str, parent_folder_id: str | None = None
    ) -> str:
        """
        Creates a folder in Google Drive.
        :param folder_name: The name of the folder to create.
        :param parent_folder_id: The ID of the parent folder (optional).
        :return: The ID of the created folder.
        """
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_folder_id:
            file_metadata["parents"] = [parent_folder_id]

        file = (
            self.connection.files()
            .create(body=file_metadata, fields="id", supportsAllDrives=True)
            .execute()
        )
        log.debug(
            f"Folder {folder_name} created under {self.generate_gdrive_folder_url(parent_folder_id)}"
        )
        return file.get("id")

    def grant_folder_access(
        self,
        folder_id: str,
        email: str,
        role: Role = Role.READER,
        send_notification_email: bool = False,
    ):
        """
        Grants access to a Google Drive folder to a user by email.
        :param folder_id: The ID of the folder to grant access to.
        :param email: The email address of the user to grant access to.
        :param role: The role to assign (reader, writer, commenter, owner).
        """
        self.connection.permissions().create(
            fileId=folder_id,
            body={
                "type": "user",
                "role": role,
                "emailAddress": email,
            },
            sendNotificationEmail=send_notification_email,
            supportsAllDrives=True,
        ).execute()
        log.debug(
            f"Granted {role} access to {email} for folder {self.generate_gdrive_folder_url(folder_id)}"
        )

    # endregion

    # region File operations

    def get_file(self, file_id: str):
        raise NotImplementedError()

    def list_files(self, folder_id: str, mime_type: str | None = None):
        """
        Lists files in a specified Google Drive folder.
        :param folder_id: The ID of the folder to search in.
        :param mime_type: Optional MIME type to filter files by.
        :return: A list of files in the specified folder.
        """
        query = f"'{folder_id}' in parents and trashed=false"
        if mime_type:
            query += f" and mimeType='{mime_type}'"

        results = (
            self.connection.files()
            .list(q=query, fields="files(id, name)", supportsAllDrives=True)
            .execute()
        )
        return results.get("files", [])

    def upload_file(
        self, src_filepath: str, folder_id: str, mime_type: str | None = None
    ):
        media = MediaFileUpload(src_filepath, mimetype=mime_type, resumable=True)
        request = self.connection.files().create(
            body={"name": os.path.basename(src_filepath), "parents": [folder_id]},
            media_body=media,
            supportsAllDrives=True,
        )
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                log.debug(f"Upload progress: {int(status.progress() * 100)}%")

        log.debug(
            f"File {src_filepath} [{naturalsize(os.path.getsize(src_filepath))}] uploaded to {self.generate_gdrive_folder_url(folder_id)}"
        )

    def download_gdrive_file(self, file_id: str, dst_filepath: str):
        request = self.connection.files().get_media(
            fileId=file_id, supportsAllDrives=True
        )

        # Stream directly to disk
        with open(dst_filepath, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        log.debug(
            f"GDrive file {file_id} downloaded to {dst_filepath} with size {naturalsize(os.path.getsize(dst_filepath))}"
        )

    def delete(self, file_id: str):
        """
        Deletes a file from Google Drive using its ID.
        :param file_id: The ID of the file to delete.
        """
        self.connection.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        log.debug(f"GDrive file with ID {file_id} deleted")

    # endregion

    # region Other utilieis

    @staticmethod
    def generate_gdrive_folder_url(folder_id: str):
        """
        Generate a valid GDrive folder URL

        Args:
            folder_id (str): Folder ID

        Returns:
            str: A valid GDrive folder URL
        """

        return f"https://drive.google.com/drive/folders/{folder_id}"

    # endregion
