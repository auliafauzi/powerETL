#!/usr/bin/python3

import pysftp
import csv
import re
import psycopg2
from datetime import datetime
from datetime import date
from datetime import timedelta
import decimal
import time
import json
import os
import xlrd

confName = './test_bulk_customer.cnf'
today = date.today()
sourcehostname = ""
sourceusername = ""
sourcepassword = ""
sourceport = ""
targetfile = ''
targethostname = ""
targetdatabase = ""
targetusername = ""
targetpassword = ""
targettable = ''
sourcelocation = ''
sourcenamepatern = ''
query = ""
logfile = ''
transformJob = {}


def transformJobList(n,func,row):
	if func == 'int4' or func == 'int8' :
		row[n] = ''.join(re.findall("\d+", row[n]))
		return row
	#['ddmmYYYY' , 'ddmmyy' , 'YYYYmmdd' , 'yymmdd' , 'dd-mm-YYYY', 'dd-mm-yy', 'YYYY-mm-dd' , 'yy-mm-dd' , 'dd/mm/YYYY' , 'dd/mm/yy' , 'YYYY/mm/dd' , 'yy/mm/dd'] 
	elif func == 'ddmmYYYY' :
		row[n] = datetime.strptime(row[n], '%d%m%Y').strftime("%Y-%m-%d")
		return row
	elif func == 'ddmmyy' :
		row[n] = datetime.strptime(row[n], '%d%m%y').strftime("%Y-%m-%d")
		return row
	elif func == 'YYYYmmdd':
		row[n] = datetime.strptime(row[n], '%Y%m%d').strftime("%Y-%m-%d")
		return row
	elif func == 'yymmdd':
		row[n] = datetime.strptime(row[n], '%y%m%d').strftime("%Y-%m-%d")
		return row
	elif func == 'dd-mm-YYYY':
		row[n] = datetime.strptime(row[n], '%d-%m-%Y').strftime("%Y-%m-%d")
		return row
	elif func == 'dd-mm-yy':
		row[n] = datetime.strptime(row[n], '%d-%m-%y').strftime("%Y-%m-%d")
		return row
	elif func == 'YYYY-mm-dd':
		return row
	elif func == 'yy-mm-dd':
		row[n] = datetime.strptime(row[n], '%y-%m-%d').strftime("%Y-%m-%d")
		return row
	elif func == 'dd/mm/YYYY' :
		row[n] = datetime.strptime(row[n], '%d/%m/%Y').strftime("%Y-%m-%d")
		return row
	elif func == 'dd/mm/yy' :
		row[n] = datetime.strptime(row[n], '%d/%m/%y').strftime("%Y-%m-%d")
		return row
	elif func == 'YYYY/mm/d':
		row[n] = datetime.strptime(row[n], '%Y/%m/%d').strftime("%Y-%m-%d")
		return row
	elif func == 'yy/mm/dd':
		row[n] = datetime.strptime(row[n], '%y/%m/%d').strftime("%Y-%m-%d")
		return row
	elif func == 'HH:MM:SS' :
		return row
	elif func == 'HH.MM.SS' :
		row[n] = datetime.strptime(row[n], '%H.%M.%S').strftime("%H:%M:%S")
		return row
	elif func == 'seconds_integer_value' :
		row[n] = secondsToTime(row[n])
		return row
	elif func == 'float4' or func == 'float8' :
		try :
			row[n] = '%.0f' % float(row[n])
		except :
			pass
		return row

def listFile_SFTP(sourcehostname, sourceport , sourceusername, sourcepassword, location, pattern) :
	file_names = []
	dir_names = []
	un_name = []
	res = []
	def store_files_name(fname):
	    file_names.append(fname) 

	def store_dir_name(dirname):
	    dir_names.append(dirname)

	def store_other_file_types(name):
	    un_name.append(name)

	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None 
	with pysftp.Connection(host=sourcehostname, port=sourceport , username=sourceusername, password=sourcepassword, cnopts=cnopts) as sftp:	
		sftp.walktree(location,store_files_name,store_dir_name,store_other_file_types,recurse=True)
		for i in range(len(file_names)) :
			if re.findall(pattern, file_names[i]) != [] :
				res.append(file_names[i])
	return res

def matchingList(list1, list2) :
	unmatched = []
	for i in list1 :
		if i not in list2 :
			unmatched.append(i)
		else :
			pass
	if unmatched == [] :
		return True, unmatched
	else :
		return False, unmatched

def insertQuery(tablename, data):
	col,_,_ = readColumn(data)
	query = "INSERT INTO %s VALUES (" % tablename
	for i in range(len(col)) :
		if i == len(col)-1 :
			query = query + "%s);"
		else :
			query = query + "%s,"
	# query = query + ");"
	return query

def readColumn(data):
	col = []
	tipe = []
	ex = []
	i = 0
	for row in data :
		if i == 0  :
			for column in row :
				# print(column)
				column = (column.replace("-","_"))
				col.append(column.replace(" ","_"))
		elif i == 1 :
			for column in row :
				# print(column)
				ex.append(column)
		elif i == 2 :
			for column in row :
				# print(column)
				tipe.append(type(column))
		else :
			pass
		i += 1 
	return col, ex, tipe

def getFromSFTP(sourcehostname, sourceport , sourceusername, sourcepassword, remoteFilePath, targetfile):
	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None 
	getFromSFTPError = True
	try :
		with pysftp.Connection(host=sourcehostname, port=sourceport , username=sourceusername, password=sourcepassword, cnopts=cnopts) as sftp:
			print("log -" + today.strftime("%d/%m/%Y") + " - Connection succesfully stablished ... \n")
			l.write("log -" + today.strftime("%d/%m/%Y") + " - Connection succesfully stablished ... \n")
			sftp.get(remoteFilePath, targetfile)
			sftp.close()
			getFromSFTPError = False
			result =  "- get file from sftp is succsess \n"
	except : 
		result = "- failed to get file from sftp, please check either the file is exist or the connection is availabe \n"
	return result, getFromSFTPError


def pushToDBJob(targethostname, targetdatabase, targetusername, targetpassword, query, targetfile, data) :
	pushToDBError = True
	print("target file :", targetfile)
	# print(data)
	try :
		conn = None
		conn = psycopg2.connect(host=targethostname, database=targetdatabase, user= targetusername, password= targetpassword)
		cur = conn.cursor()

		cur.executemany(query,data)
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

def pushToDB(targethostname, targetdatabase, targetusername, targetpassword, targetfile, data,query, TransformJob) :
	pushToDBError = True
	print("target file :", targetfile)
	try :
		conn = None
		conn = psycopg2.connect(host=targethostname, database=targetdatabase, user= targetusername, password= targetpassword)
		cur = conn.cursor()
		data.pop(0)
		ar = []
		for row in data :
			for i in TransformJob :
				row = transformJobList(int(i),TransformJob[i],row)
			ar.append(row)
			# print(row)	
		# print(query)
		# print(ar)
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

def truncateTableQuery(tablename):
	query = "TRUNCATE TABLE %s " % tablename
	return query

def queryTable(targethostname, targetdatabase, targetusername, targetpassword, targettable, query, queryname):
	queryTableError = True
	conn = None
	try :
		conn = psycopg2.connect(host=targethostname, database=targetdatabase, user= targetusername, password= targetpassword)
		cur = conn.cursor()
		cur.execute(query)
		conn.commit()
		cur.close()
		result =  "- "+queryname+" - " + targettable + " is succsess \n"
		queryTableError = False
	except : 
		result =  "- failed to "+queryname+", please check either the connection is availabe or the query is correct \n"
	finally:
		if conn is not None:
			conn.close()
	return result, queryTableError

def defineDateType(date):
	#['ddmmYYYY' , 'ddmmyy' , 'YYYYmmdd' , 'yymmdd' , 'dd-mm-YYYY', 'dd-mm-yy', 'YYYY-mm-dd' , 'yy-mm-dd' , 'dd/mm/YYYY' , 'dd/mm/yy' , 'YYYY/mm/dd' , 'yy/mm/dd'] 
	if len(date) == 8 and date[5].isdigit() and (0<int(date[0]+date[1])<32) and (0<int(date[2]+date[3])<13) and date[4] in ['1','2'] and date[5] in ['9','0'] :
		return 'ddmmYYYY'
	elif len(date) == 6 and (0<int(date[0]+date[1])<32) and (0<int(date[2]+date[3])<13) and date[4] in ['8','9','0'] :
		return 'special case'
	elif len(date) == 8 and date[5].isdigit() and (0<int(date[6]+date[7])<32) and (0<int(date[4]+date[5])<13) and date[0] in ['1','2'] and date[1] in ['9','0'] :
		return 'YYYYmmdd'
	elif len(date) == 6 and (0<int(date[4]+date[5])<32) and (0<int(date[2]+date[3])<13) and date[0] in ['9','0'] :
		return 'special case'
	elif len(date) == 10 and date[5] in ['-','/'] and (0<int(date[3]+date[4])<13) and date[6] in ['1','2'] and date[7] in ['9','0'] :
		if date[5] == '-' :
			return 'dd-mm-YYYY'
		elif date[5] == '/' :
			return 'dd/mm/YYYY'
		else : 
			pass
	elif len(date) == 8 and date[5] in ['-','/'] and (0<int(date[3]+date[4])<13) :
		return 'special case'
	elif len(date) == 10 and date[4] in ['-','/'] and (0<int(date[8]+date[9])<32) and (0<int(date[5]+date[6])<13) and date[0] in ['1','2'] and date[1] in ['9','0'] :
		if date[4] == '-' :
			return 'YYYY-mm-dd'
		elif date[4] == '/' :
			return 'YYYY/mm/dd'
		else : 
			pass	
	# elif len(date) == 8 and date[5] in ['-','/'] and (0<int(date[6]+date[7])<32) and (0<int(date[3]+date[4])<13) and date[0] in ['9','0'] :
	# 	if date[5] == '-' :
	# 		return 'yy-mm-dd'
	# 	elif date[5] == '/' :
	# 		return 'yy/mm/dd'
	# 	else : 
	# 		pass
	elif len(date) == 8 and date[5] in ['-','/'] and (0<int(date[0]+date[1])<32) and (0<int(date[3]+date[4])<13) and date[6] in ['9','0'] :
		return "ini loh"
	else :
		return 'Unrecognize date type'

def specialCaseDateType(listDate) :
	newList = []
	for i in listDate :
		if i not in newList :
			newList.append(i)
		else :
			pass
	if len(newList) < 2 :
		if len(newList[0]) == 8  and newList[0][5] == '-' and newList[0][6]+newList[0][7] in ['15','16','17','18','19','20','21','22']:
			return 'dd-mm-yy'
		elif len(newList[0]) == 8  and newList[0][5] == '-' and newList[0][0]+newList[0][1] in ['15','16','17','18','19','20','21','22']:
			return 'yy-mm-dd'
		else :
			return "Unrecognize"
	elif len(newList[1]) == 8 :
		if int(newList[1][6]+newList[1][7]) - int(newList[0][6]+newList[0][7]) == 1 :
			if newList[1][5] == '-' :
				return 'yy-mm-dd'
			elif newList[1][5] == '/' :
				return 'yy/mm/dd'
		elif int(newList[1][0]+newList[1][1]) - int(newList[0][0]+newList[0][1]) == 1 :
			if newList[1][5] == '-' :
				return 'dd-mm-yy'
			elif newList[1][5] == '/' :
				return 'dd/mm/yy'
	elif len(newList[1]) == 6 : 
		if int(newList[1][4]+newList[1][5]) - int(newList[0][4]+newList[0][5]) == 1 :
			return 'yymmdd'
		elif int(newList[1][0]+newList[1][1]) - int(newList[0][0]+newList[0][1]) == 1 :
			return 'ddmmyy'
	else :
		return 'Unrecognize date type'

def getOneColumn(data, index) : #to gather all data from one spesific column 
	column = []
	# x = 0
	for j in range(len(data)) :
		if j > 0  :	
			column.append(data[j][index])
	return column

def readExcel(file) : 
	wb = xlrd.open_workbook(file) 
	sheet = wb.sheet_by_index(0) 
	sheet.cell_value(0, 0) 
	data = []
	for i in range(sheet.nrows):
		row = []
		for j in range(len(sheet.row_values(i))) :
			try :
				value = str(decimal.Decimal(float(sheet.row_values(i)[j])))
				row.append(value)
			except :
				row.append(sheet.row_values(i)[j])
		data.append(row) 
	return data

def openCSV(targetfile, delim) :
	if delim == '':
		while True :
			delim = input('\nSelect delimiter (, or ; or |)\n')
			if delim in [',',';','|']:
				break
			else:
				print('\nPlease insert the right value\n')
				continue
	else :
		pass
	file = csv.reader(open(targetfile, "r"), delimiter = delim)
	data = []
	for row in file :
		data.append(row)
	return data

def readJobConfiguration(confName):
	global targethostname, targetdatabase, targetusername, targetpassword, targettable, sourcehostname,sourceport,sourceusername,sourcepassword,sourcelocation,sourcenamepatern,logfile,transformJob,userEmail,developerEmail,projectName,fileType,delimiter
	while  True :
		try :
			with open(confName) as json_file:
			    cnf = json.load(json_file)
			    targethostname = cnf['targetConf']['targethostname']
			    targetdatabase = cnf['targetConf']['targetdatabase']
			    targetusername = cnf['targetConf']['targetusername']
			    targetpassword = cnf['targetConf']['targetpassword']
			    targettable = cnf['targetConf']['targettable']

			    sourcehostname = cnf['sourceConf']['sourcehostname']
			    sourceport = int(cnf['sourceConf']['sourceport'])
			    sourceusername = cnf['sourceConf']['sourceusername'] 
			    sourcepassword = cnf['sourceConf']['sourcepassword']
			    sourcelocation = cnf['sourceConf']['sourcelocation']
			    sourcenamepatern = cnf['sourceConf']['sourcenamepatern']

			    projectName = cnf['common']['projectName']
			    logfile = cnf['common']['logfile']
			    transformJob = cnf['transformJob']
			    userEmail = cnf['common']['userEmail']
			    developerEmail = cnf['common']['developerEmail']
			    fileType = cnf['common']['fileType']
			    delimiter = cnf['common']['delimiter']
			break
		except : 
			print('failed to load the file, please check either the file is exist')
			continue
	os.system('clear')
	print('Load the configuration....\n')
	time.sleep(2)

def readFileList(fileListName):
	oldlist = []
	try : 
		with open(fileListName) as file :
			for line in file :
				line = line.strip()
				oldlist.append(line)
		return oldlist
	except : 
		print('failed to load the file, please check either the file is exist')


def logging(logOutput,fileprocess) :
	fileprocess.write(logOutput)
	print(logOutput)
	return




if __name__ == '__main__':
	
	readJobConfiguration(confName) #load confFile
	oldlist = readFileList('oldlist')
	data = []

	while True :
		l = open(logfile,'a')
		logging("\n///Data Checking " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " ////", l)		
		newestList = listFile_SFTP(sourcehostname,sourceport,sourceusername,sourcepassword,sourcelocation,sourcenamepatern) # check is there any new file 
		matching, new_item  = matchingList(newestList, oldlist)
		# for i in newestList :
		# 	if re.findall("xls", i) != [] :
		# 		newestList.remove(i)
		if  matching == False : # if there is new file, run this commad below :
			logging("\n///////New File Detected -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "////////\n" + "New Item : " + str(new_item), l)	
			# readJobConfiguration(confName) 
			for k in new_item : # for every new file that match pattern, do :
				if re.findall(fileType, k) != [] : #check filetype
					targetfile = k.replace(sourcelocation + '/','')
					resultGetSFTP, getFromSFTPError = getFromSFTP(sourcehostname, sourceport , sourceusername, sourcepassword, k, targetfile) # get file from sftp
					logging("\nlog -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " -- " + targetfile + " -- " + resultGetSFTP, l)
					if fileType == 'csv':
						data_piece = openCSV(targetfile,delimiter)				
					elif fileType == 'xlsx' :
						data_piece = readExcel(targetfile)
					else : 
						pass
					data_piece.pop(0) 
					for i in transformJob :
						if transformJob[i] == 'date' : #predict date
							try :
								transformJob[i] =  defineDateType(data_piece[1][int(i)])
								if transformJob[i]== 'special case' :
									column = getOneColumn(data_piece, int(i))
									transformJob[i] = specialCaseDateType(column)
								else : 
									pass
							except :
								logging("\nlog -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " -- " + targetfile + " -- " + "Failed to predict date type", l)
						else : 
							pass
					# try :				
					# 	for row in data_piece : # do transformation
					# 		print(transformJob)
					# 		for i in transformJob :
					# 			row = transformJobList(i,transformJob[i],row)
					# 		data.append(row) # append all row from all files to one variable
					# except : 
					# 	logging("\nlog -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " -- " + targetfile + " -- " + "Failed to transform the data", l)
					for row in data_piece : # append all row from all files to one variable
						data.append(row)
				else :
					pass # file type is wrong
			# print(data)
			time.sleep(5)
			print("\nPushing to DB....")
			resultpushToDB, pushToDBError = pushToDB(targethostname, targetdatabase, targetusername, targetpassword, targetfile, data,insertQuery(targettable, data), transformJob) # push to data warehouse
			logging("\nlog -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + resultpushToDB,l)
			try :
				for i in new_item :
					targetfile = i.replace(sourcelocation + '/','')
					os.remove(targetfile)
			except :
				pass
			if pushToDBError == False :
				oldlist = new_item
				with open('oldlist', 'a') as file :
					for ele in oldlist:
						file.write(ele+'\n')
			else : 
				pass
			data = []
		else : 
			print("\n\nNo new file uploaded")
		print("sleeping....")
		l.close()
		time.sleep(15)