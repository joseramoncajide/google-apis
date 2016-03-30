import io
import os
import oauth2client

from apiclient.http import MediaFileUpload
from apiclient.http import MediaIoBaseDownload
from apiclient import discovery
from oauth2client import client
from oauth2client import tools


accountId='50425604'
webPropertyId='UA-50425604-20'
customDataSourceId='K6K5TVRfRp-epCbZ9ZvmLw'

filename = inspect.getframeinfo(inspect.currentframe()).filename
PATH = os.path.dirname(os.path.abspath(filename))
os.chdir(PATH)
OUT_PATH = os.path.join(PATH, 'out')

if not os.path.exists(OUT_PATH):
    os.makedirs(OUT_PATH)

CLIENT_SECRET_FILE = os.path.join(PATH, 'client_secret.json')
#CLIENT_SECRET_FILE = 'client_secret.json'
SCOPES = ['https://www.googleapis.com/auth/analytics.edit','https://www.googleapis.com/auth/drive.file']
APPLICATION_NAME = 'El Arte de Medir'




def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.eam')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'access_token.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    flags = tools.argparser.parse_args(args=[])
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE,  scope=' '.join(SCOPES))
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_service(api_name, api_version, scope, client_secrets_path):
  """Get a service that communicates to a Google API.

  Args:
    api_name: string The name of the api to connect to.
    api_version: string The api version to connect to.
    scope: A list of strings representing the auth scopes to authorize for the
      connection.
    client_secrets_path: string A path to a valid client secrets file.

  Returns:
    A service that is connected to the specified API.
  """

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      client_secrets_path, scope=scope,
      message=tools.message_if_missing(client_secrets_path))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage(api_name + '.dat')
  flags = tools.argparser.parse_args(args=[])
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())

  # Build the service object.
  service = build(api_name, api_version, http=http)

  return service




def list_custom_data_sources(service):
    try:
       uploads = service.management().uploads().list(
          accountId=accountId,
          webPropertyId=webPropertyId,
          customDataSourceId=customDataSourceId
      ).execute()

    except TypeError, error:
  # Handle errors in constructing a query.
      print 'There was an error in constructing your query : %s' % error

    except HttpError, error:
  # Handle API errors.
      print ('There was an API error : %s : %s' %
         (error.resp.status, error.resp.reason))
# Example #2:
# The results of the list method are stored in the uploads object.
# The following code shows how to iterate through them.
    for upload in uploads.get('items', []):
      print 'Upload Id             = %s' % upload.get('id')
      print 'Upload Kind           = %s' % upload.get('kind')
      print 'Account Id            = %s' % upload.get('accountId')
      print 'Custom Data Source Id = %s' % upload.get('customDataSourceId')
      print 'Upload Status         = %s\n' % upload.get('status')    

  

def upload_cost_file(service, filename):
    try:
      media = MediaFileUpload(
          filename, # The CSV file to upload
          mimetype='application/octet-stream',
          resumable=False)

      return service.management().uploads().uploadData (
          accountId=accountId,
          webPropertyId=webPropertyId,
          customDataSourceId=customDataSourceId, 
          media_body=media).execute()

    except TypeError, error:
      # Handle errors in constructing a query.
      print 'There was an error in constructing your query : %s' % error

    except HttpError, error:
      # Handle API errors.
      print ('There was an API error : %s : %s' %
         (error.resp.status, error.resp.reason))


def download_file(service, file_id, file_name):
    #request = service.files().get_media(fileId=file_id)
    request = service.files().export_media(fileId=file_id,
                                             mimeType='text/csv')
    #fh = io.BytesIO()
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

def main():

  # Define the auth scopes to request.
    #scope = ['https://www.googleapis.com/auth/analytics.edit','https://www.googleapis.com/auth/drive.file']

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    drive_service = discovery.build('drive', 'v3', http=http)
    analytics_service = discovery.build('analytics', 'v3', http=http)

  # Authenticate and construct service.
    #analytics = get_service('analytics', 'v3', scope, './client_secret.json')

    #drive = get_service('drive', 'v3', 'https://www.googleapis.com/auth/drive.file', './client_secret.json')
  
    file_metadata = {
      'name' : 'googleAnalyticsCostData.csv',
      'mimeType' : 'application/vnd.google-apps.spreadsheet'
    }
    media = MediaFileUpload('./October-Cost-Data/2012-11-01.csv',
                        mimetype='text/csv',
                        resumable=True)
    #file = drive.files().create(body=file_metadata,
    #                                media_body=media,
    #                                fields='id').execute()
    #print 'File ID: %s' % file.get('id')

    file_id = '1vSYj_5v7uCJJcG1ywQ_u7YGwh4puamrD5QnBXqTrXNA'
    file_name = 'googleAnalyticsCostData.csv'


    #list_custom_data_sources(analytics)

    download_file(drive_service, file_id, file_name)
    #upload_cost_file(analytics, file_name)
    upload_cost_file(analytics_service, file_name)

ga_date = 'ga:date'
def check_csv_file(file):
    import csv
    with open(file, 'rU') as f:
     reader = csv.reader(f, delimiter=',')
     for row in reader:
         print(len(row))
         if len(row) != 6:
            print 'Invalid row length.'
            return
        
         if ga_date == row[0]: # if the username shall be on column 3 (-> index 2)
             print "is in file"
             return


# check_csv_file('./October-Cost-Data/2012-11-01.csv')

if __name__ == '__main__':
  main()
  

