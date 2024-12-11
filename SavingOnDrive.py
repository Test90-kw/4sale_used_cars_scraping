import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta


class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None

    def authenticate(self):
        # Load credentials directly from the JSON content
        creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
        self.service = build('drive', 'v3', credentials=creds)

    def create_folder(self, folder_name, parent_folder_id=None):
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]

        folder = self.service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

    def upload_file(self, file_name, folder_id):
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_name, resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

    def save_files(self, files):
        parent_folder_id = '11pG4Jwy1gJUbz7cILT6sfzmLD5f75nqU'  # ID of "Property Scraper Uploads"
        try:
            # Check if the folder exists and can be accessed
            folder = self.service.files().get(fileId=parent_folder_id).execute()
            print(f"Folder found: {folder['name']}")

        except Exception as e:
            print(f"Error accessing the folder with ID {parent_folder_id}: {e}")
            # Optionally, create a new folder if the existing one cannot be accessed
            print("Attempting to create a new folder...")
            parent_folder_id = self.create_folder('Cars Scraper Uploads')
            print(f"New folder created with ID: {parent_folder_id}")

        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        folder_id = self.create_folder(yesterday, parent_folder_id)

        for file_name in files:
            self.upload_file(file_name, folder_id)
        print(f"Files uploaded successfully to folder '{yesterday}' on Google Drive.")
