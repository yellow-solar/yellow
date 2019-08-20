"""" Functions for gdrive in statement sync """

import io
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import numpy as np
# Single query
# files = google_drive_service.files().list(
#         # q = "mimeType='application/vnd.google-apps.document'",
#         spaces='drive').execute().get('files',[])
# for f in files:
#     print(f)


def getFolderID(folder, service):
    """ get folder ID for a specific folder name """

    # Get response from Google Drive API using provided service
    try:
        response = service.files().list(
            q = ("(mimeType='application/vnd.google-apps.folder') and "
                    f"(name='{folder}')"),
            spaces='drive',
            fields='nextPageToken, files(id,name)').execute()
    except:
        raise

    # Extract files from the returned dictionary
    files = response.get('files',[])
    # Return error if not working
    if len(files) > 1:
        raise Exception("Folder has duplicate name in Yellow drive")
    elif len(files) == 0:
        raise Exception("Folder not found ")
    else:
        folder_id = files[0].get('id')
        return(folder_id)



def getFileIDs(folder_id, service):
    """ get file IDs for a specific folder ID """
    all_files = []
    # Get response from Google Drive API using provided service
    try:
        # loop through pages returned from API, initiate page token
        page_token = None
        while True:
            # call query with current page token
            response = service.files().list(
                q = (f"'{folder_id}' in parents"),
                    spaces='drive',
                    fields='nextPageToken, files(id,name,mimeType)',
                    pageToken=page_token).execute()
            # store next page token
            page_token = response.get('nextPageToken', None)
            # store list of file IDs and names in list main list
            all_files.extend(response.get('files',[]))
            # break out of while loop if there are no more pages
            if page_token is None:
                break
    except:
        raise Exception("Could not retrieve file id list")

    # Return message if none found
    if len(all_files) == 0:
        print("No file IDs found")
    else:
        return(all_files)

def setArchive(file_ids):
    for file_ in file_ids:
        # Check if folder, name = archive then store ID
        if (file_.get('name')=='Archive' and file_.get('mimeType')=='application/vnd.google-apps.folder'):
            archive_id = file_.get('id')
    #If none exists, throw error else return archive ID 
    if archive_id is None:
        return(None)
    else:
        return(archive_id)


def csvLoader(request):
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    return(fh, status.progress())


def getCSV(file_, service, dt_columns=None, header = 0):
    # if csv, process
    if file_.get('mimeType') == 'text/csv':
        # Make get request
        request = service.files().get_media(fileId=file_.get('id'))
        # Read into dataframe using custom function
        fh, status = csvLoader(request)
        # Check if returned status is 100%
        if status == 1:    
            fh.seek(0) # set curser to start of file
            # Process csv bytes
            df = pd.read_csv(fh, encoding='utf8', sep=",", header=header)
            # Convert datetime columns
            for dt_col in dt_columns:
                df[dt_col] = pd.to_datetime(df[dt_col])
                df[dt_col] = df[dt_col].dt.strftime('%Y-%m-%d %H:%M:%S')
            # Replace nan with None
            for col in df.columns.values:
                df[col] = np.where(df[col].isnull(),None, df[col])
        else:
            raise Exception("Failed to download file ID")
        return(df)
    else:
        return(None)


# Search folders for what you want file names
# page_token = None
# while True:
#     response = google_drive_service.files().list(
#         q = "mimeType='application/vnd.google-apps.folder'",
#         spaces='drive',
#         fields='nextPageToken, files(id,name)',
#         pageToken=page_token).execute()

#     for f in response.get('files',[]):
#         # print(f.get('name') )
#         # print(f['name'], f['mimeType'])
#         if f.get('name','') == '1. Airtel Transaction Lists':
#             print(f)

#     page_token = response.get('nextPageToken', None)
#     if page_token is None:
#         break



