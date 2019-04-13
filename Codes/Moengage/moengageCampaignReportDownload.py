#!/usr/bin/python
# encoding: utf-8
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import requests
import json
import pandas as pd
import csv
import sys
import os
import datetime
import traceback
import subprocess
import smtplib
import re
from dateutil.relativedelta import *
from hashlib import sha256
import httpie
sys.path.append("/../config")
import config
sys.path.append('/Utility/')
import processquery

def importCSV(db,sql_table,csv_loc):
    host,user,password = config.report2ServerCredential()
    processquery.excecuteSQLReport2DB("delete from "+sql_table,'otreporting')
    comm = "mysql --local-infile -h"+host+" -u"+user+" -p"+password+" "+ db +"  -e \"LOAD DATA LOCAL INFILE \'" + csv_loc +"\' INTO TABLE " + sql_table +" CHARACTER SET UTF8 FIELDS TERMINATED BY ',' IGNORE 1 LINES; SHOW WARNINGS\""
    #print comm
    os.system(comm)
    processquery.excecuteSQLReport2DB("update "+sql_table+" set reported_date = date_sub(reported_date,interval 1 day)",'otreporting')

def sendemail(to_addr_list, cc_addr_list,subject, message,login = "report@onlinetyari.com", password = "reporting@123",smtpserver='smtp.gmail.com:587',from_addr = "report"):
    header  = 'From: %s\n' % from_addr
    header += 'To: %s\n' % ','.join(to_addr_list)
    header += 'Cc: %s\n' % ','.join(cc_addr_list)
    header += 'Subject: %s\n\n' % subject
    message = header + message
    server = smtplib.SMTP(smtpserver)
    server.starttls()
    server.login(login,password)
    problems = server.sendmail(from_addr, to_addr_list, message)
    server.quit()
    return problems

def downloadReport(s,e):
    for k in range(s,e):
        Api_ID = "1OODAHJZT8MBBMCSOVLD0TUH"
        todayDate = datetime.datetime.strptime((datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d"),'%Y-%m-%d').strftime('%Y%m%d')
        #todayDate = "20180"+str(k)
        print (todayDate)
        FILENAME = todayDate + ".zip"
        SECRET_KEY = "549F7AVFKMC6"
        Signature_Key = (Api_ID + "|" + FILENAME + "|" + SECRET_KEY).encode('utf-8')
        Signature = sha256(Signature_Key).hexdigest()
        api_request = "https://api.moengage.com/dailyCampaignReportDump/" +Api_ID + "/" + FILENAME + "?Signature=" + Signature
        #print (api_request)
        comm = "sudo http " + api_request + " > /data/ot-analytics/AdHoc/Output/Moengage/CR"+ FILENAME
        #print (comm)
        os.system(comm)
        os.system("unzip /data/ot-analytics/AdHoc/Output/Moengage/CR"+ FILENAME + " -d /data/ot-analytics/AdHoc/Output/Moengage/")
    return

def renameFiles():
    for filename in os.listdir("/data/ot-analytics/AdHoc/Output/Moengage/"):
        print (filename)
        os.chdir('/data/ot-analytics/AdHoc/Output/Moengage/')
        if filename.startswith("General_Email_Onlinetyari_"):
            os.rename(filename, filename[:36]+ '.csv')
        if filename.startswith("General_In-App_Onlinetyari_"):
            os.rename(filename, filename[:37]+ '.csv')
        if filename.startswith("General_Push_Onlinetyari_"):
            os.rename(filename, filename[:35]+ '.csv')
        if filename.startswith("MV_CG__General_Push_Onlinetyari"):
            os.rename(filename, filename[:42]+ '.csv')
        if filename.startswith("MV_CG__Smart_Trigger_Push_Onlinetyari_"):
            os.rename(filename, filename[:48]+ '.csv')
        if filename.startswith("Smart_Trigger_In-App_Onlinetyari_"):
            os.rename(filename, filename[:43]+ '.csv')
        if filename.startswith("Smart_Trigger_Push_Onlinetyari_"):
            os.rename(filename, filename[:41]+ '.csv')
        if filename.startswith("Smart_Trigger_Email_Onlinetyari_"):
            os.rename(filename, filename[:42]+ '.csv')
        if filename.startswith("Geo_Fence_Push_Onlinetyari_"):
            os.rename(filename, filename[:37]+ '.csv')
        if filename.startswith("SMS_Onlinetyari_"):
            os.rename(filename,filename[:26] + '.csv')
    os.chdir('/data/ot-analytics/AdHoc/Output/Moengage/')
    os.system("sudo rm -f *.zip")
    return

def mergeGeneral_In_App_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/General_In-App_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e:
                print (e)
        df = df[['reported_date','Created At','Campaign Id','Campaign Name','Campaign Delivery Type','Conversion Goal','impressions','impressions_ANDROID','impressions_WEB','clicks','clicks_ANDROID','clicks_WEB','closed','closed_ANDROID','closed_WEB','conversion_events', 'conversion_events_ANDROID','conversion_events_WEB', 'conversions','conversions_ANDROID','conversions_WEB']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_In-App_Onlinetyari.csv',index = False)
    except Exception as e :
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeGeneral_In_App_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeGeneral_Email_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/General_Email_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e:
                print (e)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Delivery Type','Conversion Goal','Campaign Id','Parent Campaign Id','Total Sent','Hard Bounces','Soft Bounces', 'Unique Opens','Total Opens', 'Unique Clicks','Total Clicks','Unique Conversions', 'Total Conversions','Total Unsubscribes','Total Complaints','Subject Personalized','Content Personalized', 'URL Personalized', 'Links Count','Users with Emails', 'B/U/C Removed','Duplicates Removed','Invalid Emails', 'FC Removed','Peronsalization Removed']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Email_Onlinetyari.csv',index = False)
    except Exception as e :
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeGeneral_Email_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeGeneral_Push_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/General_Push_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e:
                print (e)
        df = df[['reported_date','Created At', 'Sent At', 'Campaign Name','Campaign Id','Parent Campaign Id','Campaign Delivery Type', 'Personalization', 'Number of Variations', 'Primary Conversion Goal','Campaign Attribution Window','WEB AutoDismiss', 'clicks', 'conversion_events','conversions', 'clicks_ANDROID', 'conversion_events_ANDROID', 'conversions_ANDROID', 'clicks_WEB', 'conversion_events_WEB', 'conversions_WEB', 'Installed_users_in_segment', 'After_FC_Removal', 'Active_device_tokens', 'Successfully_Sent', 'Impressions', 'Installed_users_in_segment_ANDROID','After_FC_Removal_ANDROID', 'Active_device_tokens_ANDROID', 'Successfully_Sent_ANDROID', 'Impressions_ANDROID','Installed_users_in_segment_WEB', 'After_FC_Removal_WEB', 'Active_device_tokens_WEB', 'Successfully_Sent_WEB', 'Impressions_WEB', 'Goal 1', 'Goal 1 View Through Conversions ','Goal 1 View Through Conversion Events ','Goal 1 View Through Conversions ANDROID', 'Goal 1 View Through Conversion Events ANDROID', 'Goal 1 View Through Conversions WEB', 'Goal 1 View Through Conversion Events WEB', 'Goal 1 Click Through Conversions ', 'Goal 1 Click Through Conversion Events ','Goal 1 Click Through Conversions ANDROID', 'Goal 1 Click Through Conversion Events ANDROID', 'Goal 1 Click Through Conversions WEB', 'Goal 1 Click Through Conversion Events WEB', 'Goal 1 In-Session Conversions ','Goal 1 In-Session Conversion Events ','Goal 1 In-Session Conversions ANDROID','Goal 1 In-Session Conversion Events ANDROID','Goal 1 In-Session Conversions WEB','Goal 1 In-Session Conversion Events WEB']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Push_Onlinetyari.csv',index = False)
    except Exception as e:
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeGeneral_Push_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeSmart_Trigger_In_App_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/Smart_Trigger_In-App_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e:
                print (e)
        df = df[['reported_date','Created At','Campaign Id','Campaign Name','Campaign Delivery Type','Conversion Goal','impressions','impressions_ANDROID','impressions_WEB','clicks','clicks_ANDROID','clicks_WEB','closed','closed_ANDROID','closed_WEB','conversion_events', 'conversion_events_ANDROID','conversion_events_WEB', 'conversions','conversions_ANDROID','conversions_WEB']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_In-App_Onlinetyari.csv',index = False)
    except Exception as e:
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeSmart_Trigger_In_App_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeSmart_Trigger_Email_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/Smart_Trigger_Email_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename) 
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e:
                print (e)
        df = df[['reported_date','Created At','Campaign Name','Conversion Goal','Campaign Id','Total Sent','Hard Bounces','Soft Bounces','Unique Opens','Total Opens', 'Unique Clicks','Total Clicks','Unique Conversions','Total Conversions','Total Unsubscribes','Total Complaints','Subject Personalized','Content Personalized','URL Personalized','Links Count','Users with Emails','B/U/C Removed','Duplicates Removed','Invalid Emails','FC Removed','Peronsalization Removed']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Email_Onlinetyari.csv',index = False)
    except Exception as e:
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeSmart_Trigger_Email_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeSmart_Trigger_Push_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/Smart_Trigger_Push_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e:
                print (e)
        df = df[['reported_date','Created At','Sent At', 'Campaign Name','Campaign Id','Parent Campaign Id', 'Campaign Delivery Type', 'Personalization', 'Number of Variations', 'Primary Conversion Goal','Campaign Attribution Window','WEB AutoDismiss', 'clicks', 'conversion_events','conversions', 'clicks_ANDROID', 'conversion_events_ANDROID', 'conversions_ANDROID', 'clicks_WEB', 'conversion_events_WEB', 'conversions_WEB', 'Installed_users_in_segment', 'After_FC_Removal', 'Active_device_tokens', 'Successfully_Sent', 'Impressions', 'Installed_users_in_segment_ANDROID','After_FC_Removal_ANDROID', 'Active_device_tokens_ANDROID', 'Successfully_Sent_ANDROID', 'Impressions_ANDROID','Installed_users_in_segment_WEB', 'After_FC_Removal_WEB', 'Active_device_tokens_WEB', 'Successfully_Sent_WEB', 'Impressions_WEB', 'Goal 1', 'Goal 1 View Through Conversions ','Goal 1 View Through Conversion Events ','Goal 1 View Through Conversions ANDROID', 'Goal 1 View Through Conversion Events ANDROID', 'Goal 1 View Through Conversions WEB', 'Goal 1 View Through Conversion Events WEB', 'Goal 1 Click Through Conversions ', 'Goal 1 Click Through Conversion Events ','Goal 1 Click Through Conversions ANDROID', 'Goal 1 Click Through Conversion Events ANDROID', 'Goal 1 Click Through Conversions WEB', 'Goal 1 Click Through Conversion Events WEB', 'Goal 1 In-Session Conversions ','Goal 1 In-Session Conversion Events ','Goal 1 In-Session Conversions ANDROID','Goal 1 In-Session Conversion Events ANDROID','Goal 1 In-Session Conversions WEB','Goal 1 In-Session Conversion Events WEB']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Push_Onlinetyari.csv',index = False)
    except Exception as e:
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeSmart_Trigger_Push_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeMV_CG__General_Push_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/MV_CG__General_Push_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e :
                print (e)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Id','Parent Campaign Id','Filter Description','Campaign Delivery Type','Primary Conversion Goal','A/B Test_type','Android_Variation 1_Active Targets','Android_Variation 2_Active Targets','Android_Variation 1_Impressions','Android_Variation 2_Impressions','Android_Variation 1_Clicks','Android_Variation 2_Clicks','Android_Variation 1_Conversions_Goal 1','Android_Variation 2_Conversions_Goal 1']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__General_Push_Onlinetyari.csv',index = False)
    except Exception as e :
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeMV_CG__General_Push_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeMV_CG__Smart_Trigger_Push_Onlinetyari(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/MV_CG__Smart_Trigger_Push_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e :
                print (e)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Id','Parent Campaign Id','Filter Description','Campaign Delivery Type','Primary Conversion Goal','A/B Test_type','Android_Variation 1_Active Targets','Android_Variation 2_Active Targets','Android_Variation 1_Impressions','Android_Variation 2_Impressions','Android_Variation 1_Clicks','Android_Variation 2_Clicks','Android_Variation 1_Conversions_Goal 1','Android_Variation 2_Conversions_Goal 1']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__Smart_Trigger_Push_Onlinetyari.csv',index = False)
    except Exception as e:
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeMV_CG__Smart_Trigger_Push_Onlinetyari fails due to: '+str(e))
        #print (e)
    return

def mergeSMS(s,e):
    try :
        df = pd.DataFrame()
        for k in range(s,e):
            try :
                day = (datetime.datetime.now() + relativedelta(days = -k)).strftime("%Y-%m-%d")
                filename = '/data/ot-analytics/AdHoc/Output/Moengage/SMS_Onlinetyari_' + day + '.csv'
                temp_df = pd.read_csv(filename)
                temp_df['reported_date'] = day
                df = df.append(temp_df)
                #os.system("sudo rm -f " + filename)
            except Exception as e :
                print (e)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Id','Parent Campaign Id','Target Audience','Campaign Delivery Type','Connector URL','Conversion Goal','Attempted','Successfully Sent','Failed','Conversion Events','Conversions','Conversion Rate','SMS Delivered','SMS Sent']]
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/SMS_Onlinetyari.csv',index = False)
    except Exception as e :
        sendemail(['ot-analytics@onlinetyari.com'],[],'Moengage Campaign Report Downloader ETL','mergeSMS fails due to: '+str(e))
        #print (e)
    return

def mergeALLFiles(s,e):
    mergeGeneral_In_App_Onlinetyari(s,e)
    mergeGeneral_Email_Onlinetyari(s,e)
    mergeGeneral_Push_Onlinetyari(s,e)
    mergeSmart_Trigger_In_App_Onlinetyari(s,e)
    mergeSmart_Trigger_Email_Onlinetyari(s,e)
    mergeSmart_Trigger_Push_Onlinetyari(s,e)
    mergeMV_CG__General_Push_Onlinetyari(s,e)
    mergeMV_CG__Smart_Trigger_Push_Onlinetyari(s,e)
    mergeSMS(s,e)
    return

def downloadCurDayReportZip(day):
    Api_ID = "1OODAHJZT8MBBMCSOVLD0TUH"
    todayDate = datetime.datetime.strptime(day.strftime("%Y-%m-%d"),'%Y-%m-%d').strftime('%Y%m%d')
    #todayDate = "20180"+str(k)
    print (todayDate)
    FILENAME = todayDate + ".zip"
    SECRET_KEY = "549F7AVFKMC6"
    Signature_Key = (Api_ID + "|" + FILENAME + "|" + SECRET_KEY).encode('utf-8')
    Signature = sha256(Signature_Key).hexdigest()
    api_request = "https://api.moengage.com/dailyCampaignReportDump/" +Api_ID + "/" + FILENAME + "?Signature=" + Signature
    print (api_request)
    comm = "sudo http " + api_request + " > /data/ot-analytics/AdHoc/Output/Moengage/CR"+ FILENAME
    print (comm)
    os.system(comm)
    os.system("unzip /data/ot-analytics/AdHoc/Output/Moengage/CR"+ FILENAME + " -d /data/ot-analytics/AdHoc/Output/Moengage/")
    return

def mergeALLNewFiles(day):
    renameFiles()
    day = day.strftime("%Y-%m-%d")
    print (day)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/General_In-App_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_In-App_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Campaign Id','Campaign Name','Campaign Delivery Type','Conversion Goal','impressions','impressions_ANDROID','impressions_WEB','clicks','clicks_ANDROID','clicks_WEB','closed','closed_ANDROID','closed_WEB','conversion_events', 'conversion_events_ANDROID','conversion_events_WEB', 'conversions','conversions_ANDROID','conversions_WEB']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_In-App_Onlinetyari.csv',index = False)
    except Exception as e :
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/General_Email_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Email_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Delivery Type','Conversion Goal','Campaign Id','Parent Campaign Id','Total Sent','Hard Bounces','Soft Bounces', 'Unique Opens','Total Opens', 'Unique Clicks','Total Clicks','Unique Conversions', 'Total Conversions','Total Unsubscribes','Total Complaints','Subject Personalized','Content Personalized', 'URL Personalized', 'Links Count','Users with Emails', 'B/U/C Removed','Duplicates Removed','Invalid Emails', 'FC Removed','Peronsalization Removed']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Email_Onlinetyari.csv',index = False)
    except Exception as e :
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/General_Push_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Push_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At', 'Sent At', 'Campaign Name','Campaign Id','Parent Campaign Id', 'Campaign Delivery Type', 'Personalization', 'Number of Variations', 'Primary Conversion Goal','Campaign Attribution Window','WEB AutoDismiss','clicks', 'conversion_events','conversions', 'clicks_ANDROID', 'conversion_events_ANDROID', 'conversions_ANDROID', 'clicks_WEB', 'conversion_events_WEB', 'conversions_WEB', 'Installed_users_in_segment', 'After_FC_Removal', 'Active_device_tokens', 'Successfully_Sent', 'Impressions', 'Installed_users_in_segment_ANDROID','After_FC_Removal_ANDROID', 'Active_device_tokens_ANDROID', 'Successfully_Sent_ANDROID', 'Impressions_ANDROID','Installed_users_in_segment_WEB', 'After_FC_Removal_WEB', 'Active_device_tokens_WEB', 'Successfully_Sent_WEB', 'Impressions_WEB', 'Goal 1', 'Goal 1 View Through Conversions ','Goal 1 View Through Conversion Events ','Goal 1 View Through Conversions ANDROID', 'Goal 1 View Through Conversion Events ANDROID', 'Goal 1 View Through Conversions WEB', 'Goal 1 View Through Conversion Events WEB', 'Goal 1 Click Through Conversions ', 'Goal 1 Click Through Conversion Events ','Goal 1 Click Through Conversions ANDROID', 'Goal 1 Click Through Conversion Events ANDROID', 'Goal 1 Click Through Conversions WEB', 'Goal 1 Click Through Conversion Events WEB', 'Goal 1 In-Session Conversions ','Goal 1 In-Session Conversion Events ','Goal 1 In-Session Conversions ANDROID','Goal 1 In-Session Conversion Events ANDROID','Goal 1 In-Session Conversions WEB','Goal 1 In-Session Conversion Events WEB']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Push_Onlinetyari.csv',index = False)
    except Exception as e:
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/Smart_Trigger_In-App_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_In-App_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Campaign Id','Campaign Name','Campaign Delivery Type','Conversion Goal','impressions','impressions_ANDROID','impressions_WEB','clicks','clicks_ANDROID','clicks_WEB','closed','closed_ANDROID','closed_WEB','conversion_events', 'conversion_events_ANDROID','conversion_events_WEB', 'conversions','conversions_ANDROID','conversions_WEB']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_In-App_Onlinetyari.csv',index = False)
    except Exception as e:
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/Smart_Trigger_Email_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename) 
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Email_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Campaign Name','Conversion Goal','Campaign Id','Total Sent','Hard Bounces','Soft Bounces','Unique Opens','Total Opens', 'Unique Clicks','Total Clicks','Unique Conversions','Total Conversions','Total Unsubscribes','Total Complaints','Subject Personalized','Content Personalized','URL Personalized','Links Count','Users with Emails','B/U/C Removed','Duplicates Removed','Invalid Emails','FC Removed','Peronsalization Removed']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Email_Onlinetyari.csv',index = False)
    except Exception as e:
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/Smart_Trigger_Push_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Push_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Sent At', 'Campaign Name','Campaign Id','Parent Campaign Id', 'Campaign Delivery Type', 'Personalization', 'Number of Variations', 'Primary Conversion Goal','Campaign Attribution Window','WEB AutoDismiss', 'clicks', 'conversion_events','conversions','clicks_ANDROID', 'conversion_events_ANDROID', 'conversions_ANDROID', 'clicks_WEB', 'conversion_events_WEB', 'conversions_WEB', 'Installed_users_in_segment', 'After_FC_Removal', 'Active_device_tokens', 'Successfully_Sent', 'Impressions', 'Installed_users_in_segment_ANDROID','After_FC_Removal_ANDROID', 'Active_device_tokens_ANDROID', 'Successfully_Sent_ANDROID', 'Impressions_ANDROID','Installed_users_in_segment_WEB', 'After_FC_Removal_WEB', 'Active_device_tokens_WEB', 'Successfully_Sent_WEB', 'Impressions_WEB', 'Goal 1', 'Goal 1 View Through Conversions ','Goal 1 View Through Conversion Events ','Goal 1 View Through Conversions ANDROID', 'Goal 1 View Through Conversion Events ANDROID', 'Goal 1 View Through Conversions WEB', 'Goal 1 View Through Conversion Events WEB', 'Goal 1 Click Through Conversions ', 'Goal 1 Click Through Conversion Events ','Goal 1 Click Through Conversions ANDROID', 'Goal 1 Click Through Conversion Events ANDROID', 'Goal 1 Click Through Conversions WEB', 'Goal 1 Click Through Conversion Events WEB', 'Goal 1 In-Session Conversions ','Goal 1 In-Session Conversion Events ','Goal 1 In-Session Conversions ANDROID','Goal 1 In-Session Conversion Events ANDROID','Goal 1 In-Session Conversions WEB','Goal 1 In-Session Conversion Events WEB']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Push_Onlinetyari.csv',index = False)
    except Exception as e:
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/MV_CG__General_Push_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__General_Push_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Id','Parent Campaign Id','Filter Description','Campaign Delivery Type','Primary Conversion Goal','A/B Test_type','Android_Variation 1_Active Targets','Android_Variation 2_Active Targets','Android_Variation 1_Impressions','Android_Variation 2_Impressions','Android_Variation 1_Clicks','Android_Variation 2_Clicks','Android_Variation 1_Conversions_Goal 1','Android_Variation 2_Conversions_Goal 1']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__General_Push_Onlinetyari.csv',index = False)
    except Exception as e :
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/MV_CG__Smart_Trigger_Push_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__Smart_Trigger_Push_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Id','Parent Campaign Id','Filter Description','Campaign Delivery Type','Primary Conversion Goal','A/B Test_type','Android_Variation 1_Active Targets','Android_Variation 2_Active Targets','Android_Variation 1_Impressions','Android_Variation 2_Impressions','Android_Variation 1_Clicks','Android_Variation 2_Clicks','Android_Variation 1_Conversions_Goal 1','Android_Variation 2_Conversions_Goal 1']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__Smart_Trigger_Push_Onlinetyari.csv',index = False)
    except Exception as e:
        print (e)
    try :
        df = pd.DataFrame()
        filename = '/data/ot-analytics/AdHoc/Output/Moengage/SMS_Onlinetyari_' + day + '.csv'
        temp_df = pd.read_csv(filename)
        temp_df['reported_date'] = day
        old_df = pd.read_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/SMS_Onlinetyari.csv')
        df = old_df.append(temp_df)
        os.system("sudo rm -f " + filename)
        df = df[['reported_date','Created At','Sent At','Campaign Name','Campaign Id','Parent Campaign Id','Target Audience','Campaign Delivery Type','Connector URL','Conversion Goal','Attempted','Successfully Sent','Failed','Conversion Events','Conversions','Conversion Rate','SMS Delivered','SMS Sent']]
        df = df.drop_duplicates().reset_index(drop = True)
        df = df.sort_values(by = 'reported_date',ascending = True).reset_index(drop = True)
        df.to_csv('/data/ot-analytics/Projects/Moengage/Output/reportOutput/SMS_Onlinetyari.csv',index = False)
    except Exception as e :
        print (e)
    return

def main():
    startime = datetime.datetime.now()+ relativedelta(minutes = 330)
    print (str(startime))
    #e = (datetime.datetime.now() - datetime.datetime(2018, 1, 1, 0, 0, 0, 0)).days + 1
    #s = 1
    #downloadReport(s,e)
    #renameFiles()
    #mergeALLFiles(s,e)
    yesterday = datetime.datetime.now() + relativedelta(days = -2)
    todayday = datetime.datetime.now() + relativedelta(days = -1)
    downloadCurDayReportZip(yesterday)
    mergeALLNewFiles(todayday)
    os.chdir('/data/ot-analytics/AdHoc/Output/Moengage/')
    os.system("sudo rm -f *")
    importCSV('otreporting','moe_General_In_App_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_In-App_Onlinetyari.csv')
    importCSV('otreporting','moe_General_Email_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Email_Onlinetyari.csv')
    importCSV('otreporting','moe_General_Push_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/General_Push_Onlinetyari.csv')
    importCSV('otreporting','moe_Smart_Trigger_Email_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Email_Onlinetyari.csv')
    importCSV('otreporting','moe_Smart_Trigger_In_App_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_In-App_Onlinetyari.csv')
    importCSV('otreporting','moe_Smart_Trigger_Push_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/Smart_Trigger_Push_Onlinetyari.csv')
    importCSV('otreporting','moe_MV_CG__General_Push_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__General_Push_Onlinetyari.csv')
    importCSV('otreporting','moe_MV_CG__Smart_Trigger_Push_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/MV_CG__Smart_Trigger_Push_Onlinetyari.csv')
    importCSV('otreporting','moe_SMS_Onlinetyari','/data/ot-analytics/Projects/Moengage/Output/reportOutput/SMS_Onlinetyari.csv')    
    finishtime = datetime.datetime.now()+ relativedelta(minutes = 330)
    print (str(finishtime))
    print (str(finishtime - startime))
    return

if __name__=='__main__':
    main()
    
#CREATE TABLE moe_General_In_App_Onlinetyari (`reported_date` date,`Created At` date,`Campaign Id` varchar(255),`Campaign Name` varchar(255),`Campaign Delivery Type` varchar(255),`Conversion Goal` varchar(255),`impressions` int(11),`impressions_ANDROID` int(11),`impressions_WEB` int(11),`clicks` int(11),`clicks_ANDROID` int(11),`clicks_WEB` int(11),`closed` int(11),`closed_ANDROID` int(11),`closed_WEB` int(11),`conversion_events` int(11),`conversion_events_ANDROID` int(11),`conversion_events_WEB` int(11), `conversions` int(11),`conversions_ANDROID` int(11),`conversions_WEB` int(11))

#CREATE TABLE moe_General_Email_Onlinetyari (`reported_date` date,`Created At` date,`Sent At` date,`Campaign Name` varchar(255),`Campaign Delivery Type` varchar(255),`Conversion Goal` varchar(255),`Campaign Id` varchar(255),`Parent Campaign Id` varchar(255),`Total Sent` int(11),`Hard Bounces` int(11),`Soft Bounces` int(11), `Unique Opens` int(11),`Total Opens` int(11), `Unique Clicks` int(11),`Total Clicks` int(11),`Unique Conversions` int(11), `Total Conversions` int(11),`Total Unsubscribes` int(11),`Total Complaints` int(11),`Subject Personalized` varchar(255),`Content Personalized` varchar(255), `URL Personalized` varchar(255), `Links Count` int(11),`Users with Emails` int(11), `B/U/C Removed` int(11),`Duplicates Removed` int(11),`Invalid Emails` int(11), `FC Removed` int(11) ,`Peronsalization Removed` varchar(255))

#CREATE TABLE moe_General_Push_Onlinetyari (`reported_date` date,`Created At` date, `Sent At` date, `Campaign Name` varchar(255),`Campaign Id` varchar(255),`Parent Campaign Id` varchar(255),`Custom Segment Name` varchar(255),`Platforms` varchar(255),`Campaign Delivery Type` varchar(255), `Personalization` varchar(255), `Number of Variations` int(11), `Primary Conversion Goal` varchar(255),`Campaign Attribution Window` int(11),`WEB AutoDismiss` varchar(255), `clicks` int(11), `conversion_events` int(11),`conversions` int(11), `clicks_ANDROID` int(11), `conversion_events_ANDROID` int(11), `conversions_ANDROID` int(11), `clicks_WEB` int(11), `conversion_events_WEB` int(11), `conversions_WEB` int(11), `Installed_users_in_segment` int(11), `After_FC_Removal` int(11), `Active_device_tokens` int(11), `Successfully_Sent` int(11), `Impressions` int(11), `Installed_users_in_segment_ANDROID` int(11),`After_FC_Removal_ANDROID` int(11), `Active_device_tokens_ANDROID` int(11), `Successfully_Sent_ANDROID` int(11), `Impressions_ANDROID` int(11),`Installed_users_in_segment_WEB` int(11), `After_FC_Removal_WEB` int(11), `Active_device_tokens_WEB` int(11), `Successfully_Sent_WEB` int(11), `Impressions_WEB` int(11), `Goal 1`  varchar(255) , `Goal 1 View Through Conversions` int(11),`Goal 1 View Through Conversion Events` int(11),`Goal 1 View Through Conversions ANDROID` int(11),`Goal 1 View Through Conversion Events ANDROID` int(11), `Goal 1 View Through Conversions WEB` int(11), `Goal 1 View Through Conversion Events WEB` int(11), `Goal 1 Click Through Conversions` int(11), `Goal 1 Click Through Conversion Events` int(11),`Goal 1 Click Through Conversions ANDROID` int(11), `Goal 1 Click Through Conversion Events ANDROID` int(11), `Goal 1 Click Through Conversions WEB` int(11), `Goal 1 Click Through Conversion Events WEB` int(11), `Goal 1 In-Session Conversions` int(11),`Goal 1 In-Session Conversion Events` int(11),`Goal 1 In-Session Conversions ANDROID` int(11),`Goal 1 In-Session Conversion Events ANDROID` int(11),`Goal 1 In-Session Conversions WEB` int(11),`Goal 1 In-Session Conversion Events WEB` int(11))

#CREATE TABLE moe_Smart_Trigger_In_App_Onlinetyari  (`reported_date` date,`Created At` date,`Campaign Id` varchar(255),`Campaign Name` varchar(255),`Campaign Delivery Type` varchar(255),`Conversion Goal` varchar(255),`impressions` int(11),`impressions_ANDROID` int(11),`impressions_WEB` int(11),`clicks` int(11),`clicks_ANDROID` int(11),`clicks_WEB` int(11),`closed` int(11),`closed_ANDROID` int(11),`closed_WEB` int(11),`conversion_events` int(11), `conversion_events_ANDROID` int(11),`conversion_events_WEB` int(11), `conversions` int(11),`conversions_ANDROID` int(11),`conversions_WEB` int(11))

#CREATE TABLE moe_Smart_Trigger_Email_Onlinetyari (`reported_date` date,`Created At` date,`Campaign Name` varchar(255),`Conversion Goal` varchar(255),`Campaign Id` varchar(255),`Total Sent` int(11),`Hard Bounces` int(11),`Soft Bounces` int(11),`Unique Opens` int(11),`Total Opens` int(11), `Unique Clicks` int(11),`Total Clicks` int(11),`Unique Conversions` int(11),`Total Conversions` int(11),`Total Unsubscribes` int(11),`Total Complaints` int(11),`Subject Personalized` varchar(255),`Content Personalized` varchar(255),`URL Personalized` varchar(255),`Links Count` int(11),`Users with Emails` int(11),`B/U/C Removed` int(11),`Duplicates Removed` int(11),`Invalid Emails` int(11),`FC Removed` int(11),`Peronsalization Removed` int(11))

#CREATE TABLE moe_Smart_Trigger_Push_Onlinetyari (`reported_date` date,`Created At` date,`Sent At` date, `Campaign Name` varchar(255),`Campaign Id` varchar(255),`Parent Campaign Id` varchar(255),`Platforms` varchar(255), `Campaign Delivery Type` varchar(255), `Personalization` varchar(255), `Number of Variations` varchar(255), `Primary Conversion Goal` varchar(255),`Campaign Attribution Window` int(11),`WEB AutoDismiss` varchar(255), `clicks` int(11), `conversion_events` int(11),`conversions` int(11), `clicks_ANDROID` int(11), `conversion_events_ANDROID` int(11), `conversions_ANDROID` int(11), `clicks_WEB` int(11),`conversion_events_WEB` int(11), `conversions_WEB` int(11), `Installed_users_in_segment` int(11), `After_FC_Removal` int(11), `Active_device_tokens` int(11), `Successfully_Sent` int(11), `Impressions` int(11), `Installed_users_in_segment_ANDROID` int(11),`After_FC_Removal_ANDROID` int(11), `Active_device_tokens_ANDROID` int(11), `Successfully_Sent_ANDROID` int(11), `Impressions_ANDROID` int(11),`Installed_users_in_segment_WEB` int(11), `After_FC_Removal_WEB` int(11), `Active_device_tokens_WEB` int(11), `Successfully_Sent_WEB` int(11), `Impressions_WEB` int(11), `Goal 1` varchar(255), `Goal 1 View Through Conversions` int(11),`Goal 1 View Through Conversion Events` int(11),`Goal 1 View Through Conversions ANDROID` int(11), `Goal 1 View Through Conversion Events ANDROID` int(11), `Goal 1 View Through Conversions WEB` int(11), `Goal 1 View Through Conversion Events WEB` int(11), `Goal 1 Click Through Conversions` int(11), `Goal 1 Click Through Conversion Events` int(11),`Goal 1 Click Through Conversions ANDROID` int(11), `Goal 1 Click Through Conversion Events ANDROID` int(11), `Goal 1 Click Through Conversions WEB` int(11), `Goal 1 Click Through Conversion Events WEB` int(11), `Goal 1 In-Session Conversions` int(11),`Goal 1 In-Session Conversion Events` int(11),`Goal 1 In-Session Conversions ANDROID` int(11),`Goal 1 In-Session Conversion Events ANDROID` int(11),`Goal 1 In-Session Conversions WEB` int(11),`Goal 1 In-Session Conversion Events WEB` int(11))

#CREATE TABLE moe_MV_CG__General_Push_Onlinetyari (`reported_date` date,`Created At` date,`Sent At` date,`Campaign Name` varchar(255),`Campaign Id` varchar(255),`Parent Campaign Id` varchar(255),`Filter Description` varchar(255),`Platforms` varchar(255),`Campaign Delivery Type` varchar(255),`Primary Conversion Goal` varchar(255),`A/B Test_type` varchar(255),`Android_Variation 1_Active Targets` int(11),`Android_Variation 2_Active Targets` int(11),`Android_Variation 1_Impressions` int(11),`Android_Variation 2_Impressions` int(11),`Android_Variation 1_Clicks` int(11),`Android_Variation 2_Clicks` int(11),`Android_Variation 1_Conversions_Goal 1` int(11),`Android_Variation 2_Conversions_Goal 1` int(11))

#CREATE TABLE moe_MV_CG__Smart_Trigger_Push_Onlinetyari (`reported_date` date,`Created At` date,`Sent At` date,`Campaign Name` varchar(255),`Campaign Id` varchar(255),`Parent Campaign Id` varchar(255),`Filter Description` varchar(255),`Platforms` varchar(255),`Campaign Delivery Type` varchar(255),`Primary Conversion Goal` varchar(255),`A/B Test_type` varchar(255),`Android_Variation 1_Active Targets` int(11),`Android_Variation 2_Active Targets` int(11),`Android_Variation 1_Impressions` int(11),`Android_Variation 2_Impressions` int(11),`Android_Variation 1_Clicks` int(11),`Android_Variation 2_Clicks` int(11),`Android_Variation 1_Conversions_Goal 1` int(11),`Android_Variation 2_Conversions_Goal 1` int(11))

#CREATE TABLE moe_SMS_Onlinetyari (`reported_date` date,`Created At` date,`Sent At` date,`Campaign Name` varchar(255),`Campaign Id` varchar(255),`Parent Campaign Id` varchar(255),`Target Audience` varchar(255),`Campaign Delivery Type` varchar(255),`Connector URL` varchar(255),`Conversion Goal` varchar(255),`Attempted` int(11),`Successfully Sent` int(11),`Failed` int(11),`Conversion Events` int(11),`Conversions` int(11),`Conversion Rate` int(11),`SMS Delivered` int(11),`SMS Sent` int(11))
