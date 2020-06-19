import pysftp
import csv
import psycopg2
# import xlrd
import time
import os
import common
import transform


# def excelPushToDB(targethostname, targetdatabase, targetusername, targetpassword, targetfile) :
# 	pushToDBError = True
# 	print("target file :", targetfile)
# 	try :
# 		conn = None
# 		with open("transformed_ids_pac.csv", "w") as myfile:
# 			conn = psycopg2.connect(host=targethostname, database=targetdatabase, user= targetusername, password= targetpassword)
# 			cur = conn.cursor()
# 			# file = csv.reader(open(targetfile, "r"), delimiter = ';')
# 			file = xlrd.open_workbook(targetfile)
# 			sheet = file.sheet_by_index(0) 
# 			sheet.cell_value(0, 0) 
# 			wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
# 			ar = []
# 			for row in range(sheet.nrows):
# 				transformJob_ids_pac(row)
# 				ar.append(row)
# 				print(row)	
# 				# cur.execute( "INSERT INTO ods.testing_aulia VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",row)
# 				# cur.execute(query,row)
# 				# conn.commit()
# 			cur.executemany(query,ar)
# 			conn.commit()
# 			cur.close()
# 			pushToDBError = False
# 		result =  "- push to data warehouse is succsess \n"
# 	except : 
# 		result =  "- failed to push the data into database, please check either the connection is availabe or the transformation is correct \n"
# 	finally:
# 		if conn is not None:
# 			conn.close()
# 	return result, pushToDBError


def getFromSFTP(sourcehostname, sourceport , sourceusername, sourcepassword, remoteFilePath, targetfile):
	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None 
	getFromSFTPError = True
	try :
		with pysftp.Connection(host=sourcehostname, port=sourceport , username=sourceusername, password=sourcepassword, cnopts=cnopts) as sftp:
			print("log -" + today.strftime("%d/%m/%Y") + "- Connection succesfully stablished ... \n")
			l.write("log -" + today.strftime("%d/%m/%Y") + "- onnection succesfully stablished ... \n")
			sftp.get(remoteFilePath, targetfile)
			sftp.close()
			getFromSFTPError = False
			result =  "- get file from sftp is succsess \n"
	except : 
		result = "- failed to get file from sftp, please check either the file is exist or the connection is availabe \n"
	return result, getFromSFTPError

def pushToDB_old(cnf, targetfile) :
	pushToDBError = True
	print("target file :", targetfile)
	try :
		conn = None
		with open("transformed_ocbi_pac.csv", "w") as myfile:
			conn = psycopg2.connect(host=cnf['targetConf']['targethostname'], database=cnf['targetConf']['targetdatabase'], user= cnf['targetConf']['targetusername'], password= cnf['targetConf']['targetpassword'])
			cur = conn.cursor()
			file = csv.reader(open(targetfile, "r"), delimiter = ';')
			next(file, None)
			wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
			ar = []
			for row in file :
				transformJob_ocbi_narindo(row)
				ar.append(row)
				print(row)	
				# cur.execute( "INSERT INTO ods.testing_aulia VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",row)
				# cur.execute(query,row)
				# conn.commit()
			writeCSV(ar)
			cur.executemany(query,ar)
			conn.commit()
			cur.close()
			pushToDBError = False
		result =  "- push to data warehouse is succsess \n"
	except : 
		result =  "- failed to push the data into database, please check either the connection is availabe or the transformation is correct \n"
	finally:
		if conn is not None:
			conn.close()
	return result, pushToDBError

def pushToDB(cnf, targetfile, data,query) :
	pushToDBError = True
	print("target file :", targetfile)
	print("query :", query)
	# print('transformjob1 ==', cnf['transformjob'])
	# print('here')
	try :
		conn = None
		conn = psycopg2.connect(host=cnf['targetConf']['targethostname'], database=cnf['targetConf']['targetdatabase'], user= cnf['targetConf']['targetusername'], password= cnf['targetConf']['targetpassword'])
		cur = conn.cursor()
		data.pop(0)
		ar = []
		for row in data :
			# print('transformjob2 ==', cnf['transformjob'])
			for i in cnf['transformjob'] :
				# print(cnf['transformjob'][i])
				row = transform.transformJobList(i,cnf['transformjob'][i],row)
			print(row)
			ar.append(row)
			# print(row)	
		# print(query)
		# print('ar: ', ar)
		# writeCSV(ar)
		cur.executemany(query,ar)
		conn.commit()
		cur.close()
		pushToDBError = False
		result =  "- push to data warehouse is succsess \n"
	except : 
		result =  "- failed to push the data into database, please check either the connection is availabe or the transformation is correct \n"
	finally:
		if conn is not None:
			conn.close()
	return result, pushToDBError


def createTableQuery(column, kv, targettable):
	query = "CREATE TABLE %s (" % targettable
	print(query)
	for i in range(len(column)) :
		if i == len(column)-1:
			query = query +'\"'+ column[i] +'\" '+ kv[i] + " " + "NULL" +" "
		else :
			query = query +'\"'+ column[i] +'\" '+ kv[i] +" " + "NULL," +" "
	query = query + ");"
	print("query:", query)
	return query

def select_query(tablename) :
	query = "SELECT * from %s" % tablename

def truncateTableQuery(tablename):
	query = "TRUNCATE TABLE %s " % tablename
	return query

def dropTableQuery(tablename):
	query = "DROP TABLE %s " % tablename
	return query

def insertQuery(tablename, data):
	col,_,_ = common.readColumn(data)
	query = "INSERT INTO %s VALUES (" % tablename
	for i in range(len(col)) :
		if i == len(col)-1 :
			query = query + "%s);"
		else :
			query = query + "%s,"
	# query = query + ");"
	return query

def queryGetDatname():
	return "SELECT datname FROM pg_database;"

def queryGetSchema():
	query  = "SELECT distinct nspname AS schemaname FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace) WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND relkind='r';"
	return query


def queryTable(cnf,query, queryname):
	queryTableError = True
	query_result = None
	conn = None
	try :
		conn = psycopg2.connect(host=cnf['targetConf']['targethostname'], database=cnf['targetConf']['targetdatabase'], user= cnf['targetConf']['targetusername'], password= cnf['targetConf']['targetpassword'])
		cur = conn.cursor()
		cur.execute(query)
		conn.commit()
		if queryname == 'Create table' :
			query_result = "Create table success"
		else :
			query_result = cur.fetchall() 
		cur.close()
		result =  "\n- "+queryname+" - " + cnf['targetConf']['targettable'] + " is succsess \n"
		queryTableError = False
	except : 
		result =  "\n- failed to "+queryname+", please check either the connection is availabe or the query is correct \n"
	finally:
		if conn is not None:
			conn.close()
	return query_result, result, queryTableError


def createTablefunc(data, cnf) :
	column, example,_ = common.readColumn(data)
	kv = common.defineColumn(column, example)
	query = createTableQuery(column, kv, cnf['targetConf']['targettable'])
	query_result, result, queryCreateTableError = queryTable(cnf, query, 'Create table')
	return result, queryCreateTableError
