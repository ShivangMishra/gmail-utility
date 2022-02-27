import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
MSG_ID_FILENAME = 'mgs_ids.dat'
messages = [] # all messages

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
    prompt = 'Press 1 to load message ids from gmail api.\nPress 2 to load message ids from local backup.\n'
      
    msgIds = []
    choice = int(input(prompt))
    if choice == 1:
        msgList = loadMsgList(service)
        msgIds = [msg['id'] for msg in msgList]
        saveMessageIdsToFile(msgIds)

    if choice == 2:
        msgIds = loadMessageIdsFromFile()
    
    input('Press Enter to load messages using the message ids >')
    messages = loadMessages(msgIds, service)
    
    input('Press Enter to sort senders ')
    sortedSenders = getSortedSenders() 
    print('\nSorted list of senders\n')
   
    for item in sortedSenders:
        print(item)
    

def loadMsgList(service, user_id='me'):
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
    return msg_list


def loadMessages(msgIds, service, user_id='me'):
    maxRequestsPerBatch = 45 # max limit is 100, 50+ is not considered safe.
    # get the messages in batches
    batch=service.new_batch_http_request()
    requestsInBatch = 0 
    for index in range(len(msgIds)):
        #print('Preparing request for message number ' + str(index) + ' ...')
        
        m_id = msgIds[index]
        request = service.users().messages().get(userId=user_id,id=m_id) 
        batch.add(request=request, callback=saveMessage)
        requestsInBatch += 1
        # execute batch when the batch is filled enough or no more ids are left
        if requestsInBatch == maxRequestsPerBatch or index == len(msgIds) - 1:
            print('executing batch request...')
            batch.execute()
            requestsInBatch = 0
            batch = service.new_batch_http_request()
            print('Batch executed successfully\n')
    
    print('Number of Retrived messages : ' + str(len(messages)))
    return messages
    # print(senders)

def saveMessageIdsToFile(msgIds, filename=MSG_ID_FILENAME):
    print('Saving message ids locally in file : ' + filename)
    with open(filename,'w') as f:
        for id in msgIds:
            f.write(str(id))


def loadMessageIdsFromFile(filename=MSG_ID_FILENAME):
    msgIds = []
    with open(filename, "r") as f:
        while True:
            try:
                msgIds.append(f.readline())
            except EOFError:
                print('\nError occured while reading file\n')
                break
    return msgIds

def saveMessage(request_id, response, exception):
    if exception is not None:
        print('Error occured for request id  = ' + str(request_id))
        input('Press Enter to continue >')
    
    message = response
    messages.append(message)


def getSortedSenders():
    """"returns a list of items of form (sender, messagesCount)
    sorted in decreasing order of messagesCount"""
    sendersMap = {}
    print('\nCounting emails per sender...')
    for msg in messages:
        msg_id = msg['id']
        msg_headers = msg['payload']['headers']
        msg_from = filter(lambda hdr: hdr['name'] == 'From', msg_headers)
        msg_from = list(msg_from)[0]
        sender = msg_from['value']
        if sender in sendersMap:
            sendersMap[sender] +=1
        else:
            sendersMap[sender] = 1
            
    print('Sorting senders on the basis of number of emails...')
    sortedSenders = sorted(sendersMap.items(), key=lambda item:-int(item[1]))
    
    return sortedSenders


if __name__ == '__main__':
    main()