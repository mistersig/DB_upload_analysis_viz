import psycopg2 as dbcooper
import pprint
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import datetime
#from config import config


def cooperChanges(dbName, query, update):
	''' Initial function to update table '''
	#connection string to String
	connString = "host='localhost' dbname='%s' user='postgres' password='' " %(dbName)
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (connString)
	print "\n"
	#get a connection, exception will be raised here if no connection made
	conn = dbcooper.connect(connString)
	print 'connected to db \ncreating ' + dbName

	#will return object than can be used to query | cursor()
	queryCon = conn.cursor()
	print "Please QA the following"
	print "\n"
	print query
	print update
	print "\n"
	approve = raw_input("Are the queries correct? (yes | no) : ").lower()

	if approve == 'yes':
		print "updating...."
		pass
	else:
		print "Query incorrect please revise. Script aborted"
		sys.exit()

	queryCon.execute(update)

	#queryCon.execute(query)
	
	conn.commit()

	queryCon.close()
	conn.close()
	print "Update Complete"
	print "\n"


def cooperVIZ(dbName, query):
	''' Used map plot for a quick and dity graph '''
	#connection string to String
	connString = "host='localhost' dbname='%s' user='postgres' password='' " %(dbName)
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (connString)
	print "\n"
	#get a connection, exception will be raised here if no connection made
	conn = dbcooper.connect(connString)
	#print 'connected to db \ncreating ' + dbName

	#will return object than can be used to query | cursor()
	queryCon = conn.cursor()
	queryCon.execute(query)
	#print query


	data = queryCon.fetchall()

	pData = []
	xTickMarks = []

	for row in data:
		#print row[1]
		pData.append(int(row[1]))
		xTickMarks.append(str(row[0]))
	#print pData

	#plot for graph 
	fig = plt.figure()
	ax = fig.add_subplot(111)
	#need to work and adjust spacing X labels still get cut off a bit 
	fig.set_size_inches(8,11.5)
	fig.autofmt_xdate()

	#necessary variables
	ind = np.arange(len(pData))    #the x locations 
	width = 0.20 #width of the bars
	## the bars
	rects1 = ax.bar(ind, pData, width,
                color='black',
                error_kw=dict(elinewidth=2,ecolor='red'))

	# axes and labels
	ax.set_xlim(-width,len(ind)+width)
	#ax.set_xlim()
	maxV = np.amax(pData)
	ax.set_ylim(0,maxV)
	#Labels
	ax.set_ylabel('Counts')
	ax.set_xlabel('Crimes')
	ax.set_title('Crimes Committed by Count Jan 14 - Mar 14')

	ax.set_xticks(ind+width)
	xtickNames = ax.set_xticklabels(xTickMarks)
	plt.setp(xtickNames, rotation=45, fontsize=10)


	plt.show()
	#print data 
	#conn.commit()
	queryCon.close()
	conn.close()
	print "Graph Complete"
	print "\n"

	



######## MAIN ##########

#DB NAME
dbName = 'chicagocrime2014'

# variables to change and update


primary_type = 'Grand Theft Auto'
arrest = 'FALSE'

#for cooper changes
sqlQuery = "select * from crime2014q1 where Primary_Type = 'MOTOR VEHICLE THEFT' and arrest = false limit 10;"
#for cooper changes 
sqlUpdate = """update crime2014q1 set Primary_Type = '%s' where Primary_Type = 'MOTOR VEHICLE THEFT' and arrest = false;""" %(primary_type)
#print sqlUpdate #test

#query for VIZ
sqlQViz = """select distinct primary_type,count(primary_type) from crime2014q1 group by primary_type order by count desc;"""



cooperChanges(dbName,sqlQuery,sqlUpdate)

cooperVIZ(dbName,sqlQViz)

















