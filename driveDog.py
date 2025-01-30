import os
import time
import io
import logging
import logging.handlers
import filecmp
import shutil
import dotenv
import hashlib
from datetime import datetime


from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


class DriveSync:
    def __init__(self, credentials_path, drive_folder_id, log_drive_folder_id, local_folder, output_folder):
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        self.service = build('drive', 'v3', credentials=self.credentials)

        self.drive_folder_id = drive_folder_id
        self.log_drive_folder_id = log_drive_folder_id
        self.local_folder = local_folder
        self.output_folder = output_folder

        self.setup_logging()

        self.ensure_dir(local_folder)
        self.ensure_dir(output_folder)

    def setup_logging(self):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        while logger.handlers:
            logger.removeHandler(logger.handlers[0])

        rotating_file_handler = logging.handlers.RotatingFileHandler(
            filename='drive_sync.log',
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=5
        )
        rotating_file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        rotating_file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        logger.addHandler(rotating_file_handler)
        logger.addHandler(console_handler)

    def ensure_dir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def get_drive_files(self, folder_id=None):

        if folder_id is None:
            folder_id = self.drive_folder_id

        logging.info(f"Fetching Drive files for folder {folder_id}")
        results = []
        page_token = None
        
        while True:
            try:
                query = f"'{folder_id}' in parents and trashed = false"
                response = self.service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, md5Checksum, modifiedTime)',
                    pageToken=page_token
                ).execute()
                
                for item in response.get('files', []):
                    if item['name'].startswith('drive_sync.log'):
                        logging.info(f"Skipping log file in Drive: {item['name']}")
                        continue

                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        sub_items = self.get_drive_files(item['id'])
                        for sub_item in sub_items:
                            sub_item['path'] = os.path.join(item['name'], sub_item.get('path', ''))
                        results.extend(sub_items)
                    else:
                        item['path'] = item['name']
                        results.append(item)

                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                    
            except Exception as e:
                logging.error(f"Error fetching Drive files: {str(e)}")
                raise

        return results

    def download_file(self, file_id, file_path):

        logging.info(f"Downloading file from Drive to {file_path}")
        try:
            request = self.service.files().get_media(fileId=file_id)
            file_handle = io.BytesIO()
            downloader = MediaIoBaseDownload(file_handle, request)
            done = False
            
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logging.debug(f"Download progress: {int(status.progress() * 100)}%")

            file_handle.seek(0)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            with open(file_path, 'wb') as f:
                f.write(file_handle.read())

            logging.info(f"Finished downloading file: {file_path}")
            return True
        except Exception as e:
            logging.error(f"Error downloading file {file_path}: {str(e)}")
            return False

    def move_files(self, intake_dir, output_dir):

        for root, dirs, files in os.walk(intake_dir):
            relative_path = os.path.relpath(root, intake_dir)
            target_dir = os.path.join(output_dir, relative_path)
            self.ensure_dir(target_dir)
            
            for file in files:
                src_file_path = os.path.join(root, file)
                dst_file_path = os.path.join(target_dir, file)

                if os.path.exists(dst_file_path):
                    logging.info(f"Overwriting existing file in output: {dst_file_path}")
                    os.remove(dst_file_path)
                
                shutil.copy2(src_file_path, dst_file_path)
                logging.info(f"Copied file {file} to {dst_file_path}")

    def upload_log_file(self, local_log_path, folder_id):

        if not os.path.exists(local_log_path):
            logging.warning(f"Log file does not exist locally: {local_log_path}")
            return False

        try:
            file_metadata = {
                'name': os.path.basename(local_log_path),  
                'parents': [folder_id]
            }
            media = MediaFileUpload(local_log_path, mimetype='text/plain', resumable=True)

            created_file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            logging.info(
                f"Uploaded '{local_log_path}' to Drive folder '{folder_id}'. File ID: {created_file.get('id')}"
            )
            return True
        except Exception as e:
            logging.error(f"Error uploading log file '{local_log_path}' to Drive: {str(e)}")
            return False

    def sync(self):
        logging.info("=== Starting sync process ===")
        try:
            drive_files = self.get_drive_files()

            for drive_file in drive_files:
                relative_path = drive_file.get('path', drive_file['name'])
                local_path = os.path.join(self.output_folder, relative_path)

                if os.path.exists(local_path):
                    local_md5 = self.md5(local_path)
                    drive_md5 = drive_file.get('md5Checksum')
                    if local_md5 == drive_md5:
                        logging.info(f"Skipping {local_path}, already up-to-date.")
                        continue 

                self.download_file(drive_file['id'], local_path)

            self.move_files(self.local_folder, self.output_folder)

            for root, dirs, files in os.walk(self.local_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted cahced file: {file_path}")
                    except Exception as e:
                        logging.error(f"Error deleting cached {file_path}: {str(e)}")

            logging.info("=== Sync process complete ===")

            self.upload_log_file('drive_sync.log', self.log_drive_folder_id)

        except Exception as e:
            logging.error(f"Sync error: {str(e)}")

    def md5(self, file_path):

        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
        except Exception as e:
            logging.error(f"Error computing MD5 for {file_path}: {e}")
            return None
        return hash_md5.hexdigest()


    def start_continuous_sync(self, interval=60):
        logging.info(f"Starting continuous sync; interval: {interval} seconds.")
        while True:
            try:
                self.sync()
                logging.info(f"Sync completed at {datetime.now()}")
                time.sleep(interval)
            except KeyboardInterrupt:
                logging.info("Sync stopped by user.")
                break
            except Exception as e:
                logging.error(f"Continuous sync error: {str(e)}")
                time.sleep(interval)


if __name__ == "__main__":

    dotenv.load_dotenv()

    CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH")
    DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
    LOG_DRIVE_FOLDER_ID = os.getenv("LOG_DRIVE_FOLDER_ID")
    LOCAL_FOLDER = os.getenv("LOCAL_FOLDER")
    OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER")

    SYNC_INTERVAL = 10  
    
    sync = DriveSync(CREDENTIALS_PATH, DRIVE_FOLDER_ID, LOG_DRIVE_FOLDER_ID, LOCAL_FOLDER, OUTPUT_FOLDER)
    sync.start_continuous_sync(SYNC_INTERVAL)
