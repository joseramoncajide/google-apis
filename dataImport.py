import io
import os
import httplib2
import oauth2client

from apiclient.http import MediaFileUpload
from apiclient.http import MediaIoBaseDownload
from apiclient import discovery
from oauth2client import client
from oauth2client import tools





accountId='50425604'
webPropertyId='UA-50425604-20'
customDataSourceId='iqavbsG8Tdivt5kzjSktbg'
schema = ['ga:date','ga:source','ga:medium','ga:campaign','ga:keyword','ga:impressions','ga:adClicks','ga:adCost']


#filename = inspect.getframeinfo(inspect.currentframe()).filename
#PATH = os.path.dirname(os.path.abspath(__file__))
PATH = '/Users/JOSE/Documents/GitHub/google-apis'
os.chdir(PATH)
OUT_PATH = os.path.join(PATH, 'out')

SCOPES = ['https://www.googleapis.com/auth/analytics.edit','https://www.googleapis.com/auth/drive.file']
APPLICATION_NAME = 'El Arte de Medir'
CLIENT_SECRET_FILE = os.path.join(PATH, 'client_secrets.json')


if not os.path.exists(OUT_PATH):
    os.makedirs(OUT_PATH)


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
        else: # Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


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

    for upload in uploads.get('items', []):
      print 'Upload Id             = %s' % upload.get('id')
      print 'Upload Kind           = %s' % upload.get('kind')
      print 'Account Id            = %s' % upload.get('accountId')
      print 'Custom Data Source Id = %s' % upload.get('customDataSourceId')
      print 'Upload Status         = %s\n' % upload.get('status')    


def check_csv_file(file):
    import csv
    with open(file, 'rU') as f:
        dictReader = csv.DictReader(f, delimiter=',')
        # print dictReader.fieldnames
        count = 0
        for row in dictReader.fieldnames:
            #print(row)
            if row in schema:
                # print "found %s %s"%(row, row in row)
                count += 1
        if count != len(schema):
            return False
        else:
            return True
 
    #check_csv_file('out/googleAnalyticsCostData.csv')

def create_csv_schema(file):
    import csv
    f = open(file, 'wb')
    wr = csv.writer(f, quoting=csv.QUOTE_ALL)
    wr.writerow(schema)


def upload_cost_file(service, filename):
    try:
      valid = check_csv_file(filename)
      if valid:
          media = MediaFileUpload(
              filename, 
              mimetype='application/octet-stream',
              resumable=False)

          return service.management().uploads().uploadData (
              accountId=accountId,
              webPropertyId=webPropertyId,
              customDataSourceId=customDataSourceId, 
              media_body=media).execute()
      else:
        print 'Wrong CSV fomat in file: %s' % filename

    except TypeError, error:
      # Handle errors in constructing a query.
      print 'There was an error in constructing your query : %s' % error

    except HttpError, error:
      # Handle API errors.
      print ('There was an API error : %s : %s' %
         (error.resp.status, error.resp.reason))


def download_file(service, file_id, file_name):

    request = service.files().export_media(fileId=file_id,
                                             mimeType='text/csv')
    fh = io.FileIO(os.path.join(OUT_PATH, file_name), 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

def upload_file(service, file_name, mime_type):
    file_metadata = {
      'name' : file_name,
      'mimeType' : 'application/vnd.google-apps.spreadsheet'
    }
    media = MediaFileUpload(file_name,
                        mimetype=mime_type,
                        resumable=True)
    file = service.files().create(body=file_metadata,
                                    media_body=media,
                                    fields='id').execute()
    print 'File ID: %s' % file.get('id')   
    return file.get('id')

def save_file_id(file_id):
    f = open( '.fileid', 'w' )
    f.write( file_id )
    f.close()
    
def read_file_id():
    if os.path.exists('.fileid'):
        f = open( '.fileid', 'r' )
        file_id = f.read()
        return file_id


def main():

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    drive_service = discovery.build('drive', 'v3', http=http)
    analytics_service = discovery.build('analytics', 'v3', http=http)
    
    file_name = 'googleAnalyticsData.csv'
    
    file_id = read_file_id()
    if file_id:
        download_file(drive_service, file_id, file_name)
        upload_cost_file(analytics_service, os.path.join(OUT_PATH, file_name))
    else:
        create_csv_schema(file_name)
        file_id = upload_file(drive_service, file_name, mime_type='text/csv')
        save_file_id(file_id)            


if __name__ == '__main__':
  main()
  

