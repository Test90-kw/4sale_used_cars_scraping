from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import os.path
import pickle

def get_yesterday_date():
    """Get yesterday's date in the required format."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def authenticate_google_drive():
    """Authenticate with Google Drive API."""
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None
    
    # Load saved credentials if they exist
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # Refresh credentials if expired
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    # If no credentials available, let user log in
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        
        # Save credentials for future use
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)

def copy_folder(service, source_folder_id, dest_folder_id, folder_name):
    """Copy a folder from source to destination."""
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

def main():
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

if __name__ == '__main__':
    main()
