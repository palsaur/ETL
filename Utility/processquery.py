#utility function for Codes to execute queries without having to bother about create db connections or credentials. 
#MainSQLDb : Main Database where your project data is stored and no one have edit access.
#ProcessedSQLDb : Processed Database where ETLS dump the processed data. 

import MySQLdb
import os
import sys
import pandas as pd
import time
sys.path.append("/Utility/")
import config
import random

def extractdatafromMainDB(query,db): #Extract & Return Data from Main DB
	try :
        #get MainSQLDb Instance Credentials
		host,user,password = config.getMainDBCredentials()
		db = db
		outFile = "/Projects/Output/adhoc"+str(int(time.time()))+str(random.randint(1,10000))+".csv"
		comm = "mysql -u"+user + " -p"+password + " -h "+host + " " + db +" --execute=\""+str(query)+"\" | sed s/'\t/,/g' > "+str(outFile)
		os.system(comm)
		df5 = pd.read_csv(outFile)
		os.system("rm -f " +outFile)
	except Exception as e:
			df5 = pd.DataFrame()
	return df5

def extractdatafromProcessedDB(query,db): #Extract & Return Data from Processed DB
	try :
        #get ProcessedSQLDb Instance Credentials
		host,user,password = config.getProcessedDBCredentials()
		db = db
		outFile = "/Projects/Output/adhoc"+str(int(time.time()))+str(random.randint(1,10000))+".csv"
		comm = "mysql -u"+user + " -p"+password + " -h "+host + " " + db +" --execute=\""+str(query)+"\" | sed s/'\t/,/g' > "+str(outFile)
		os.system(comm)
		df5 = pd.read_csv(outFile)
		os.system("rm -f " +outFile)
	except Exception as e:
			df5 = pd.DataFrame()
	return df5

def excecuteSQLProcessedDB(query,db):  #Execute DDL/DML based queries, in processed db
    #get MainSQLDb Instance Credentials
	host,user,password = config.getProcessedDBCredentials()
	db1 = MySQLdb.connect(host,user,password,db)
	cur=db1.cursor()
	sql = query
	cur.execute(sql)
	db1.commit()
	db1.close()
	return

def importCSVProcessedDB(db,csv_loc,sql_table):  #Import CSV directly into the required table
	host,user,password = config.getProcessedDBCredentials()
	comm = "mysql --local-infile -h"+host+" -u"+user+" -p"+password+" "+ db +"  -e \"LOAD DATA LOCAL INFILE \'" + csv_loc +"\' INTO TABLE " + sql_table +" CHARACTER SET UTF8 FIELDS TERMINATED BY ',' IGNORE 1 LINES; SHOW WARNINGS\""
	#print comm
	os.system(comm)
	os.system("rm -f " +csv_loc)
	return
