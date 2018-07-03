import psycopg2 as dbcooper
import pprint
import pandas as pd
#from config import config



def cooperCreate(dbName):
	''' Initial function to create and load data  '''
	#connection string to String
	connString = "host='localhost' user='postgres' password='' "
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (connString)
	#get a connection, exception will be raised here if no connection made
	conn = dbcooper.connect(connString)
	print 'connected to db \ncreating ' + dbName

	#needed to create a db psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
	conn.set_isolation_level(dbcooper.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
	#will return object than can be used to query | cursor()
	queryCon = conn.cursor()

	#execute to CREATE TABLE 
	queryCon.execute("CREATE DATABASE " + dbName)

	# retrieve records from db 
	#records = queryCon.fetchall()
	#close out
	queryCon.close()
	conn.close()
	print "DB %s has been created " %(dbName)
	print "\n"
	

	#pprint.pprint(records)


def cooperTables(dbName, fName):
	''' Function to create table and load data  '''
	connString = "host='localhost' dbname='%s' user='postgres' password='' " %(dbName)
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (connString)

	#get a connection, exception will be raised here if no connection made
	conn = dbcooper.connect(connString)

	#will return object than can be used to query | cursor()
	queryCon = conn.cursor()

	#create spatial references for point data 
	queryCon.execute("CREATE EXTENSION postgis")
	
	#ID,Case Number,Date,Time,Block,Primary Type,Description,Location Description,Arrest,Domestic,District,Ward,Community Area,FBI Code,Latitude,Longitude
	queryExpress = '''CREATE TABLE Crime2014q1
	(
	ID serial primary key,
	Case_Number varchar(64),
	Date date,
	time time,
	Block varchar(64),
	Primary_Type varchar(64),
	Description varchar(64),
	Location_Description varchar(64),
	Arrest boolean,
	Domestic boolean,
	District smallint,
	Ward smallint,
	Community_Area smallint,
	FBI_Code varchar(64),
	Latitude float(8),
	Longitude float(8)
	); '''

	#execute query 
	queryCon.execute(queryExpress)
	#queryCon.commit()
	print "TABLE CREATED"
	print "\n"
	print "\n"

	conn.commit()
	queryCon.close()


def cooperUpload(fName):
	connString = "host='localhost' dbname='%s' user='postgres' password='' " %(dbName)
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (connString)
	#get a connection, exception will be raised here if no connection made
	conn = dbcooper.connect(connString)
	#will return object than can be used to query | cursor()
	queryCon = conn.cursor()
	print "\n"
	print "\n"
	#print fName
	table_name = 'crime2014q1'
	file_object = fName
	SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
    CSV
    HEADER
    DELIMITER AS ','
    """
	queryCon.copy_expert(sql=SQL_STATEMENT % table_name, file=file_object)
	conn.commit()
	queryCon.close()
	print "UPLOAD SUCCESS!!!!"
	print "\n"
	print "\n"





##################
#Global Variable 

pathFile = '/Users/Sigfrido/Documents/GitHub/ChiCrimeDB14/dbUpload2Graph/data/quarter/'

fileName = pathFile + 'crimes201401_201703.csv'


f = open(fileName)





#DB NAME
dbName = 'chicagocrime2014'


#create DB
cooperCreate(dbName)
#tables 
cooperTables(dbName,f)
cooperUpload(f)

f.close()




