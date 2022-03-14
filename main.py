import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pickle

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
MSG_FILENAME = 'msgs.dat'
MSG_ID_FILENAME = 'mgs_ids.dat'
LOG_FILE_NAME = 'gmai-log.txt'


def main(): 
    creds = loadCreds()
    # Call the Gmail API
    service = build('gmail', 'v1', credentials=creds)
    print('Authenticated successfully')
    
    prompt = 'Enter 1 to load message ids from gmail api.\nEnter 2 to load message ids from local backup.\n? '
    choice  = input(prompt)
    if choice == '1':
        msgList = loadMsgList(service)
        msgIds = [msg['id'] for msg in msgList]
        saveMsgIdsToFile(msgIds)
    elif choice == '2':
        msgIds = loadMsgIdsFromFile()

    prompt = 'Enter 1 to load messages from gmail api.\nEnter 2 to load messages from local backup.\n? '
    choice = input(prompt)
    if choice == '1':    
        messages = loadMessages(msgIds, service)
        saveMessagesToFile(messages)

    elif choice == '2':
        messages = loadMessagesFromFile()
        if(len(messages) < len(msgIds)):
            print('Incomplete backup. Loading the missing messages from Gmail API...')
            toBeLoaded = [msgId for msgId in msgIds if not msgId in messages]
            print('Number of messages to be loaded : ' + str(len(toBeLoaded)))
            messages |= loadMessages(toBeLoaded, service)
            print('Loaded missing messages. Updating local databse...')
            saveMessagesToFile(messages)
            print('Updated.\n')

    while True:
        choice = input('Enter 1 to sort the senders.\n Enter 2 to PERMANENTLY delete\nEnter 3 to Exit\n? ')
        if choice == '1':
            sortedSenders = getSortedSenders(messages) 
            print('\nSorted list of senders\n')
            for item in sortedSenders:
                print(item)
    
        elif choice == '2':
            sender = input("Enter the sender's email address : ")
            deleteMessages(service, messages=messages, sender=sender)

        elif choice == '3':
            return

def loadCreds(tokenFileName='token.json', credsFileName='credentials.json'):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(tokenFileName):
        creds = Credentials.from_authorized_user_file(tokenFileName, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credsFileName, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(tokenFileName, 'w') as token:
            token.write(creds.to_json())
    return creds

def loadMsgList(service, user_id='me'):
    """loads all the message ids from the Gmail API."""
    msg_list = [] # only stores message ids and thread ids, not the complete messages
    maxResultsPerPage = 450 # can be upto 500, I have kept it small for simplicity
    
    request = service.users().messages().list(userId=user_id, maxResults=maxResultsPerPage)
    response = request.execute()
    print("Loading message ids...")
    msg_list.extend(response['messages']) 
    print('Loaded message ids : ' + str(len(msg_list)))
    nextPageToken = response['nextPageToken'] # used to get the next page of the results

    # get the message ids
    while nextPageToken is not None:
        request = service.users().messages().list(userId=user_id, maxResults=maxResultsPerPage, pageToken=nextPageToken)
        response = request.execute()
        msg_list.extend(response['messages'])
        print('Loaded message ids : ' + str(len(msg_list)))
        if not 'nextPageToken' in response:
            break
        nextPageToken = response['nextPageToken']
    
    print('Number of retrieved message ids : ' + str(len(msg_list)))
    return msg_list

def saveMsgIdsToFile(msgIds, filename=MSG_ID_FILENAME):
    with open(filename, 'wb') as f:
        pickle.dump(msgIds, f)
       
    print('\n' + str(len(msgIds)) + ' Message Ids saved successfully to file : ' + filename)

def loadMsgIdsFromFile(filename=MSG_ID_FILENAME):
    msgIds = []
    with open(filename, 'rb') as f:
        msgIds = pickle.load(f)
    print(str(len(msgIds)) + ' Message Ids loaded successfully from file : ' + filename)
    return msgIds
   

def loadMessages(msgIds, service, user_id='me'):
    """ Loads the messages from Gmail API. Returns a dictionary with (key, value) = (message id, message object) """
    maxRequestsPerBatch = 45 # max limit is 100, 50+ is not considered safe.
    saveStep = 10000 # save messages after each 10k messages
    
    batch=service.new_batch_http_request() # get the messages in batches
    requestsInBatch = 0 
    toBeSaved = 0 # number of messages which have not been saved. Messages are saved when toBeSaved >= saveStep
    messages = {} # to store the retrieved messages
    failedIds = [] # to store ids of the failed messages
    
    for index in range(len(msgIds)):
        #print('Preparing request for message number ' + str(index) + ' ...')
        
        m_id = msgIds[index]
        request = service.users().messages().get(userId=user_id,id=m_id) 
        batch.add(request=request, callback=lambda request_id, response, exception:handleResponse(request_id, response, exception, messages, failedIds))
        requestsInBatch += 1
        # execute batch when the batch is filled enough or no more ids are left
        if requestsInBatch == maxRequestsPerBatch or index == len(msgIds) - 1:
            print('Executing batch with ' + str(requestsInBatch) + ' requests...')
            batch.execute()
            toBeSaved += requestsInBatch
            print('Batch executed successfully. Loaded ' + str(requestsInBatch) + ' messages')
            print(' Total messages loaded : ' + str(len(messages)) + '\n')
            if toBeSaved >= saveStep:
                print('Saving current progress...')
                saveMessagesToFile(messages)
                print('Saved current progress.\n')
                toBeSaved = 0
            requestsInBatch = 0
            batch = service.new_batch_http_request()
            
            
    print('Number of Retrieved messages : ' + str(len(messages)))
    print('Number of failed messages : ' + str(len(failedIds)))
    return messages
    # print(senders)

def handleResponse(request_id, response, exception, messages, failedIds):
    if exception is not None:
        with open(LOG_FILE_NAME, 'a') as logfile:
            logfile.write(str(request_id) + "\t" + str(exception) + '\n')
        failedIds.append(request_id)
        return
    messages[response['id']] = response        
    

def saveMessagesToFile(msgs, filename=MSG_FILENAME):
    with open(filename, 'wb') as f:
        pickle.dump(len(msgs), f)
        pickle.dump(msgs, f)
        # for i in range(len(msgs)):
        #     pickle.dump(msgs[i], f)
    print('\n' + str(len(msgs)) + ' Messages saved successfully to file : ' + filename)

def loadMessagesFromFile(filename=MSG_FILENAME):
    msgs = {}
    with open(filename, 'rb') as f:
        n = int(pickle.load(f))
        msgs = pickle.load(f)
    print(str(len(msgs)) + ' Messages loaded successfully from file : ' + filename)
    return msgs

def getSortedSenders(messages):
    """"returns a list of items of form (sender, messagesCount)
    sorted in decreasing order of messagesCount"""
    sendersMap = {}
    print('\nCounting emails per sender...')
    print(str(len(messages)))
    for msg in messages.values():
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

def deleteMessages(service, user_id='me', messages={}, sender=''):
    msgIds = getMessageIds(messages, sender)
    if sender == '':
        print('SENDER NOT SPECIFIED')
        return
    if len(msgIds) == 0:
        print('No messages from sender : ' + str(sender))
        return
        
    print('PERMANENTLY Deleting all messages(' + str(len(msgIds)) + ') from sender : ' + sender)
    request = service.users().messages().batchDelete(userId=user_id, body={'ids': msgIds}) 
    # executing delete request requires the highest authority(scope) in OAuth client
    # Please see https://developers.google.com/gmail/api/auth/scopes
    request.execute() 
    for msgId in msgIds:
        messages.pop(msgId, None) # using hashmap, reduced time complexity
    saveMessagesToFile(messages)
    print('Successfully deleted all messages from sender : ' + sender)

def getMessageIds(messages, sender):
    """returns the message ids of the messages sent by the 'sender' email address"""
    msgIds = []
    for msg in messages.values():
        msg_id = msg['id']
        msg_headers = msg['payload']['headers']
        msg_from = filter(lambda hdr: hdr['name'] == 'From', msg_headers)
        msg_from = list(msg_from)[0]
        senderData = msg_from['value']
        
        if "<" + sender + ">" in senderData: # the senderData contains email in the form <email address>
            msgIds.append(msg_id)

    return msgIds

if __name__ == '__main__':
    main()