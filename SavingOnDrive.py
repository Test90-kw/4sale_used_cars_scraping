import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta
import ssl
import time
from googleapiclient.errors import HttpError

class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None

    def authenticate(self):
        creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
        self.service = build('drive', 'v3', credentials=creds)

    def create_folder(self, folder_name, parent_folder_id=None):
        try:
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"

            results = self.service.files().list(q=query, spaces='drive').execute()
            folders = results.get('files', [])

            if folders:
                print(f"Folder '{folder_name}' already exists.")
                return folders[0].get('id')

            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]

            folder = self.service.files().create(body=file_metadata, fields='id').execute()
            print(f"Created folder '{folder_name}'.")
            return folder.get('id')
        except Exception as e:
            print(f"Error creating folder '{folder_name}': {e}")
            return None

    def upload_file(self, file_name, folder_id):
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_name, resumable=True)
        retries = 5

        for i in range(retries):
            try:
                file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(f"Uploaded {file_name} successfully.")
                return file.get('id')
            except ssl.SSLEOFError as ssl_error:
                print(f"SSL Error while uploading {file_name}. Retrying in {2 ** i} seconds...")
                time.sleep(2 ** i)
            except HttpError as http_error:
                if http_error.resp.status in [403, 500, 503]:
                    print(f"HTTP Error {http_error.resp.status} while uploading {file_name}. Retrying {i+1}/{retries}...")
                    time.sleep(2 ** i)
                else:
                    print(f"Upload failed due to HTTP Error: {http_error}")
                    break
            except Exception as e:
                print(f"Unexpected error during upload of {file_name}: {e}")
                break
        else:
            print(f"Failed to upload {file_name} after {retries} attempts.")
            return None

    def save_files(self, files):
        parent_folder_ids = ['1S5jVZ7bFSEhr2aWKlSZjIRkUlNhGdYG2', '1EOZyBDFZWobN8QvznAVOWc2myS8BDfph']
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        for parent_folder_id in parent_folder_ids:
            folder_id = self.create_folder(yesterday, parent_folder_id)
            if folder_id:
                for file_name in files:
                    self.upload_file(file_name, folder_id)
    
        print(f"Files uploaded successfully to both parent folders under '{yesterday}' on Google Drive.")


