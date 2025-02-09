from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import os
import json
from google.oauth2 import service_account

def get_yesterday_date():
    """Get yesterday's date in the required format."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def authenticate_google_drive():
    """Authenticate with Google Drive API using service account."""
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    try:
        # Get credentials from environment variable
        credentials_info = json.loads(os.environ.get('ANALYSIS_COPY'))
        
        # Create credentials object directly
        creds = Credentials(
            token=None,
            client_id=credentials_info['installed']['client_id'],
            client_secret=credentials_info['installed']['client_secret'],
            token_uri=credentials_info['installed']['token_uri'],
            refresh_token=os.environ.get('GOOGLE_REFRESH_TOKEN')  # You'll need to add this secret
        )
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        
        return build('drive', 'v3', credentials=creds)
    
    except Exception as e:
        print(f"Authentication error: {str(e)}")
        raise

def copy_folder(service, source_folder_id, dest_folder_id, folder_name):
    """Copy a folder from source to destination."""
    try:
        # Create new folder in destination
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [dest_folder_id]
        }
        new_folder = service.files().create(body=folder_metadata).execute()
        
        # List all files in source folder
        query = f"'{source_folder_id}' in parents"
        results = service.files().list(q=query).execute()
        files = results.get('files', [])
        
        # Copy each file to new folder
        for file in files:
            copied_file = {
                'name': file['name'],
                'parents': [new_folder['id']]
            }
            service.files().copy(
                fileId=file['id'],
                body=copied_file
            ).execute()
    except Exception as e:
        print(f"Error in copy_folder: {str(e)}")
        raise

def main():
    try:
        # Extract folder IDs from the URLs
        source_folder_id = "11pG4Jwy1gJUbz7cILT6sfzmLD5f75nqU"
        dest_folder_id = "1vqooBw99wWVr2SdaQeyRBtIdgpeZMlRo"
        
        # Get yesterday's date
        yesterday = get_yesterday_date()
        
        # Authenticate and build service
        service = authenticate_google_drive()
        
        # Search for folder with yesterday's date in source
        query = f"name='{yesterday}' and mimeType='application/vnd.google-apps.folder' and '{source_folder_id}' in parents"
        results = service.files().list(q=query).execute()
        folders = results.get('files', [])
        
        if not folders:
            print(f"No folder found with name {yesterday}")
            return
        
        # Copy the folder
        source_date_folder = folders[0]
        copy_folder(service, source_date_folder['id'], dest_folder_id, yesterday)
        print(f"Successfully copied folder {yesterday}")
        
    except Exception as e:
        print(f"An error occurred in main: {str(e)}")
        raise

if __name__ == '__main__':
    main()
