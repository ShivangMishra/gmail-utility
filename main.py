import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

messages = []
def main():
    
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # Call the Gmail API
    service = build('gmail', 'v1', credentials=creds)
    
    print('Authenticated successfully')
    input('Press Enter to start loading messages.')
    
    loadAllMessages(service)


def loadAllMessages(service, user_id='me'):
    msg_list = [] # only stores message ids and thread ids, not the complete messages
    maxResultsPerPage = 50 # can be upto 500, I have kept it small for simplicity
    
    request = service.users().messages().list(userId=user_id, maxResults=maxResultsPerPage)
    response = request.execute()
    
    msg_list.extend(response['messages']) 
    
    nextPageToken = response['nextPageToken'] # used to get the next page of the results

    # get the message ids
    while nextPageToken is not None:
        request = service.users().messages().list(userId=user_id, maxResults=maxResultsPerPage, pageToken=nextPageToken)
        response = request.execute()
        msg_list.extend(response['messages'])
        print(str(len(msg_list)))
        if not 'nextPageToken' in response:
            break
        nextPageToken = response['nextPageToken']
    
    print('Number of retrieved message ids : ' + str(len(msg_list)))

    maxRequestsPerBatch = 45 # max limit is 100, 50+ is not considered safe.
    
    # get the messages in batches
    batch=service.new_batch_http_request()
    requestsInBatch = 0 
    for index in range(len(msg_list)):
        #print('Preparing request for message number ' + str(index) + ' ...')
        
        m_id = msg_list[index]['id']
        request = service.users().messages().get(userId=user_id,id=m_id) 
        batch.add(request=request, callback=handle_response)
        requestsInBatch += 1
        # execute batch when the batch is filled enough or no more ids are left
        if requestsInBatch == maxRequestsPerBatch or index == len(msg_list) - 1:
            print('executing batch request...')
            batch.execute()
            requestsInBatch = 0
            batch = service.new_batch_http_request()
            print('Batch executed successfully\n')
    
    print('Number of Retrived messages : ' + str(len(messages)))
    # print(senders)

def handle_response(request_id, response, exception):
    if exception is not None:
        print('Error occured for request id  = ' + str(request_id))
        input('Press Enter to continue >')
    
    message = response
    messages.append(message)
 

if __name__ == '__main__':
    main()