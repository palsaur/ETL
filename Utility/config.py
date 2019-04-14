#utility function to store credentials. To be git ignored

#MainSQLDb : Main Database where your project data is stored and no one have edit access.
#ProcessedSQLDb : Processed Database where ETLS dump the processed data. 

def getMoengageCredentials():
    PROJECT_ID = "YOUR-MOENGAGE-PROJECT-ID"
    SECRET_KEY ="YOUR-MOENGAGE-SECRET-KEY"
    API_HASH_TOKEN = "YOUR-MOENGAGE-AUTHORIZATION-API-TOKEN"
    return PROJECT_ID,SECRET_KEY,API_HASH_TOKEN

def getMainDBCredentials():
    host = "YOUR-MAIN-DB-HOSTURL"
    user = "YOUR-MAIN-DB-USERNAME"
    password = "YOUR-MAIN-DB-PASSWORD"
    return host,user,password

def getProcessedDBCredentials():
    host = "YOUR-PROCESSED-DB-HOSTURL"
    user = "YOUR-PROCESSED-DB-USERNAME"
    password = "YOUR-PROCESSED-DB-PASSWORD"
    return host,user,password

def getExceptionEmailCredential():
    #enable SMTP settings for the mail id. 
    email_id = "youmailid@gmail.com"
    password = "password"
    return email_id,password