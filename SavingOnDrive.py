import os
import json
from google.oauth2.service_account import Credentials  # For authenticating with Google APIs using a service account
from googleapiclient.discovery import build  # For building the Google Drive API client
from googleapiclient.http import MediaFileUpload  # For handling file uploads
from datetime import datetime, timedelta  # For working with date and time (e.g., getting yesterday's date)
import ssl  # For handling SSL errors
import time  # For implementing retry delays
from googleapiclient.errors import HttpError  # For handling specific Google API HTTP errors

# This class handles authentication and uploading files to Google Drive
class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict  # The JSON credentials dictionary used for authentication
        self.scopes = ['https://www.googleapis.com/auth/drive']  # Scope for full access to Google Drive
        self.service = None  # Placeholder for the authenticated Google Drive service instance

    # Authenticates and initializes the Google Drive API service using provided credentials
    def authenticate(self):
        creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
        self.service = build('drive', 'v3', credentials=creds)

    # Creates a new folder in Google Drive (if it doesn't already exist)
    def create_folder(self, folder_name, parent_folder_id=None):
        try:
            # Build query to search for folder by name and optionally by parent folder
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"

            # Search for existing folder with the same name
            results = self.service.files().list(q=query, spaces='drive').execute()
            folders = results.get('files', [])

            # If folder already exists, return its ID
            if folders:
                print(f"Folder '{folder_name}' already exists.")
                return folders[0].get('id')

            # Folder metadata for creation
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]

            # Create the folder and return its ID
            folder = self.service.files().create(body=file_metadata, fields='id').execute()
            print(f"Created folder '{folder_name}'.")
            return folder.get('id')
        except Exception as e:
            # Handle errors in folder creation
            print(f"Error creating folder '{folder_name}': {e}")
            return None

    # Uploads a single file to the specified folder on Google Drive, with retry logic for SSL/HTTP errors
    def upload_file(self, file_name, folder_id):
        file_metadata = {'name': file_name, 'parents': [folder_id]}  # Metadata for file upload
        media = MediaFileUpload(file_name, resumable=True)  # File to be uploaded with resumable upload enabled
        retries = 5  # Number of retry attempts in case of upload failure

        for i in range(retries):
            try:
                # Attempt to upload the file
                file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(f"Uploaded {file_name} successfully.")
                return file.get('id')
            except ssl.SSLEOFError as ssl_error:
                # Retry on SSL error with exponential backoff
                print(f"SSL Error while uploading {file_name}. Retrying in {2 ** i} seconds...")
                time.sleep(2 ** i)
            except HttpError as http_error:
                # Retry on specific HTTP errors with exponential backoff
                if http_error.resp.status in [403, 500, 503]:
                    print(f"HTTP Error {http_error.resp.status} while uploading {file_name}. Retrying {i+1}/{retries}...")
                    time.sleep(2 ** i)
                else:
                    print(f"Upload failed due to HTTP Error: {http_error}")
                    break  # Do not retry on other HTTP errors
            except Exception as e:
                # Catch-all for unexpected errors
                print(f"Unexpected error during upload of {file_name}: {e}")
                break
        else:
            # If all retry attempts fail
            print(f"Failed to upload {file_name} after {retries} attempts.")
            return None

    # Saves (uploads) a list of files to two specific parent folders under a dated subfolder (yesterday's date)
    def save_files(self, files):
        parent_folder_ids = ['1S5jVZ7bFSEhr2aWKlSZjIRkUlNhGdYG2', '1EOZyBDFZWobN8QvznAVOWc2myS8BDfph']  # IDs of target parent folders
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')  # Get yesterday's date in YYYY-MM-DD format

        for parent_folder_id in parent_folder_ids:
            # Create (or get existing) dated subfolder in each parent folder
            folder_id = self.create_folder(yesterday, parent_folder_id)
            if folder_id:
                for file_name in files:
                    # Upload each file to the created dated subfolder
                    self.upload_file(file_name, folder_id)
    
        print(f"Files uploaded successfully to both parent folders under '{yesterday}' on Google Drive.")
