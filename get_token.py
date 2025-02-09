from google_auth_oauthlib.flow import InstalledAppFlow
import json
import os

# Get credentials from environment variable
credentials_json = os.environ.get('ANALYSIS_COPY')
if not credentials_json:
    raise ValueError("ANALYSIS_COPY environment variable not found")

credentials_info = json.loads(credentials_json)

SCOPES = ['https://www.googleapis.com/auth/drive']
flow = InstalledAppFlow.from_client_config(
    credentials_info,
    SCOPES,
    redirect_uri='http://localhost'
)

creds = flow.run_local_server(port=0)
print("\nRefresh Token:", creds.refresh_token)
