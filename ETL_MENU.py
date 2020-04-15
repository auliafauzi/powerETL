#!/usr/bin/python3

import pysftp
import csv
import re
import psycopg2
from datetime import datetime
from datetime import date
from datetime import timedelta
import xlrd
import time
import os
from getpass import getpass
import decimal


today = date.today()
sourcehostname = "sftp?"
sourceusername = "zoho?"
sourcepassword = "password"
sourceport = 27027
# remoteFilePath = 'ETL/ids_narindo/ids_narindo_' + today.strftime("%Y%m%d")+'.csv'
remoteFilePath = 'ETL/Unipin/DIRECT_TOPUP_11022020.csv'
targetfile = 'test.csv'
# targetfile = input("input file :  ")

targethostname = 'localhost'
targetdatabase = "ops_query"
targetusername = "dwh_op"
targetpassword = "password?"
targettable = 'ods.external_ids_pac'
query = "INSERT INTO %s VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)" % targettable
logfile = './log_'+ targettable +'.log'
# logfile = '/home/dwhis/ETL/log/log_'+ targettable +'.log'
# logError = '/home/dwhis/ETL/log/log_failed.log'

menulist = {}
menulist[1] = 'Create New Table to Database'
menulist[2] = 'Insert and Transform file to Database'
menulist[3] = 'Create ETL Project'
menulist[4] = 'Query on Database'
menulist[5] = 'Set and save configuration'

SourceList = {}
SourceList[1] = 'Local CSV'
SourceList[2] = 'Local XLSX'
SourceList[3] = 'Remote CSV via SFTP'
SourceList[4] = 'Remote XLSX via SFTP'
SourceList[5] = 'Manual input'

def defineTargetDB():
	global targethostname, targetdatabase, targetusername, targetpassword, targettable
	os.system('clear')
	print('\nConfigure Target Database....\n')
	time.sleep(1)
	targethostname = input('\nInsert target Host:\n')
	targetdatabase = input('\nInsert target database name:\n')
	targetusername = input('\nInsert username:\n')
	targetpassword = getpass('\nInsert password:\n')
	targettable = input('\nInsert target table name \n(format : schema.tablename)\n(example : ods.oc_jurnal):\n')

def mainScreen():
	print('//////////////////////////////////')
	print('////// Welcome To PowerETL ///////')
	print('//////////////////////////////////\n')
	print('Developed by Aulia Fauzi Rahman')
	print('aulia.fauzi.rahman1@gmail.com\n\n\n\n')
	print('Please give credit to author for any ussage of this application\n')
	print('Initializing...')



def transformJob_ids_pac(row):
	row[0] = datetime.strptime(row[0], '%Y%m%d').strftime("%Y-%m-%d")
	# row[12] = int(row[12])
	# row[13] = int(row[13])
	return row

def transformJob_ocbi_narindo(row):
	row[1] = datetime.strptime(row[1], '%d/%m/%y').strftime("%Y-%m-%d")
	row[2] = datetime.strptime(row[2], '%H.%M.%S').strftime("%H:%M:%S")
	row[7] = ''.join(re.findall("\d+", row[7]))
	row[10] = ''.join(re.findall("\d+", row[10]))
	return row


def transformJob_PAC(row) :
	row[0] = datetime.strptime(row[0], '%Y%m%d').strftime("%Y-%m-%d")
	row[1] = secondsToTime(row[1])
	row[10] = ''.join(re.findall("\d+", row[10]))
	row[11] = ''.join(re.findall("\d+", row[11]))
	try :
		row[6] = '%.0f' % float(row[6])
		row[13] = '%.0f' % float(row[13])
	except :
		pass
	return row

def secondsToTime(n): 
    return str(timedelta(seconds = int(n)/10)) 
      
def checkContainStr(string) :
	if re.findall("[a-zA-z]", string) != [] :
		return 'Contain Character'
	elif re.findall("[@_!#$%^&*()<>-?/\|}{~:]", string) != [] :
		return 'Contain Special Character'
	elif re.findall("[.,]", string) != [] :
		return 'Contain Decimal Separator'
	else :
		return 'Digit only'

def transformRemoveChar(n):
	row[n] = ''.join(re.findall("\d+", row[n]))

def transformJobList(n,func,row):
	if func == 1 :
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
	elif func == 6 :
		try :
			row[n] = '%.0f' % float(row[n])
		except :
			pass
		return row

def defineTransformJob(data) :
	os.system('clear')
	print('Reading the file....')
	time.sleep(1)
	print('\nConfiguring transformation job....')
	time.sleep(2)
	TransformJob = {}
	for i in range(len(data[0])) :
		# print('column ' + str(i+1) + ": " + data[0][i] + '\n example value: ' + data[1][i] + "\n")
		while True :
			os.system('clear')
			inputkv= input('\nType for column ' + str(i+1) + ":\nColumn name: " + data[0][i] + '\nexample value: ' + data[1][i]+ "\n(OPTIONS : varchar/int4/int8/float4/float8/date/timestamp)\ninput:\n")
			if inputkv == 'varchar' :
				break
			elif inputkv in ['int4', 'int8'] :
				excolumn = getSample(data, i)
				excolumn = ''.join(excolumn)
				chkResult = checkContainStr(excolumn)
				print("\n"+chkResult+ "\n")
				if chkResult == 'Contain Character' or chkResult == 'Contain Special Character' :
					choices = input("\nValue has non digit character, remove any non digit character and transform to integer? (y/n)\n")
					if choices == 'y' :
						TransformJob[i] = 1
						break
					else :
						continue
				elif chkResult == 'Contain Decimal Separator' :
					print('\nValue contain decimal separator, suggesting float for variable type')
					continue
				else :
					break
			elif inputkv in ['float4', 'float8'] :
				excolumn = getSample(data, i)
				excolumn = ''.join(excolumn)
				chkResult = checkContainStr(excolumn)
				print("\n"+chkResult+ "\n")
				if chkResult == 'Contain Character' or chkResult == 'Contain Special Character' :
					choices = input("\nValue has non digit character, remove any non digit character and transform to integer? (y/n)\n")
					if choices == 'y' :
						TransformJob[i] = 1
						break
					else :
						continue
				else :
					break
			elif inputkv == 'date' :
				print('\nProcessing date value... Figuring what kind date type it is...')
				time.sleep(2)
				print('\nSample value : ', data[1][i])
				choices = defineDateType(data[1][i])
				if choices == 'special case' :
					column = getOneColumn(data, i)
					choices = specialCaseDateType(column)
				elif choices == 'Unrecognize date type' :
					print('\nDate type is: ', choices)
					time.sleep(1)
					print('\nPlease either make sure data type is date or check the file...\n')
					time.sleep(3)
					continue
				else :
					pass
				print('\nDate type is: ', choices)
				while True :
					choices2 = input('\nAgree?(y/n)\n')
					if choices2 in ['y'] :				
						break
					elif choices2 in ['n'] :	
						while True:
							print('(OPTIONS : ddmmYYYY , ddmmyy , YYYYmmdd , yymmdd , dd-mm-YYYY , dd-mm-yy , YYYY-mm-dd , yy-mm-dd , dd/mm/YYYY , dd/mm/yy , YYYY/mm/dd , yy/mm/dd)')
							choices = input('\ninput:\n')
							if choices in ['ddmmYYYY' , 'ddmmyy' , 'YYYYmmdd' , 'yymmdd' , 'dd-mm-YYYY', 'dd-mm-yy', 'YYYY-mm-dd' , 'yy-mm-dd' , 'dd/mm/YYYY' , 'dd/mm/yy' , 'YYYY/mm/dd' , 'yy/mm/dd'] :
								break
							else :
								choices = ''
								print('\nPLEASE INSERT INPUT CORRECTLY !\n')
								print('(ddmmYYYY , ddmmyy , YYYYmmdd , yymmdd , dd-mm-YYYY , dd-mm-yy , YYYY-mm-dd , yy-mm-dd , dd/mm/YYYY , dd/mm/yy , YYYY/mm/dd , yy/mm/dd)')
								continue
					else :
						print('\nPLEASE INSERT INPUT CORRECTLY !')
						time.sleep(1)
						continue
				TransformJob[i] = choices
				break
			elif inputkv == 'timestamp' :
				print('\nSample value : ', data[1][i])
				print('What pattern that match value above ?')
				print('(OPTIONS : HH:MM:SS , HH.MM.SS, seconds_integer_value)')
				while True:
					choices = input('\ninput:\n')
					if choices in ['HH:MM:SS' , 'HH.MM.SS', 'seconds_integer_value'] :
						break
					else :
						choices = ''
						print('\nPLEASE INSERT INPUT CORRECTLY !')
						print('(HH:MM:SS , HH.MM.SS, seconds_integer_value)\n')
						continue
				TransformJob[i] = choices
				break
			else :
				print('\nPLEASE INSERT INPUT CORRECTLY !\n')
				time.sleep(1.5)
				continue
	return TransformJob

def getOneColumn(data, index) : #to all data from one spesific column 
	column = []
	# x = 0
	for j in range(len(data)) :
		if j > 0  :
			excolumn.append(data[j][index])
	return column

def getSample(data, index) : #to grab sample (15 if possible) from one spesific column 
	excolumn = []
	# x = 0
	for j in range(len(data)) :
		if j > 0 and j < 16 :
			excolumn.append(data[j][index])
	return excolumn

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

def excelPushToDB(targethostname, targetdatabase, targetusername, targetpassword, targetfile) :
	pushToDBError = True
	print("target file :", targetfile)
	try :
		conn = None
		with open("transformed_ids_pac.csv", "w") as myfile:
			conn = psycopg2.connect(host=targethostname, database=targetdatabase, user= targetusername, password= targetpassword)
			cur = conn.cursor()
			# file = csv.reader(open(targetfile, "r"), delimiter = ';')
			file = xlrd.open_workbook(targetfile)
			sheet = file.sheet_by_index(0) 
			sheet.cell_value(0, 0) 
			wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
			ar = []
			for row in range(sheet.nrows):
				transformJob_ids_pac(row)
				ar.append(row)
				print(row)	
				# cur.execute( "INSERT INTO ods.testing_aulia VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",row)
				# cur.execute(query,row)
				# conn.commit()
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

def readExcel(file) : 
	wb = xlrd.open_workbook(file) 
	sheet = wb.sheet_by_index(0) 
	sheet.cell_value(0, 0) 

	row = []
	data = []
	for i in range(sheet.nrows):
		for j in range(len(sheet.row_values(i))) :
			try :
				value = str(decimal.Decimal(float(sheet.row_values(i)[j])))
				row.append(value)
			except :
				row.append(sheet.row_values(i)[j])
		data.append(row) 
	return data

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

def pushToDB_old(targethostname, targetdatabase, targetusername, targetpassword, targetfile) :
	pushToDBError = True
	print("target file :", targetfile)
	try :
		conn = None
		with open("transformed_ocbi_pac.csv", "w") as myfile:
			conn = psycopg2.connect(host=targethostname, database=targetdatabase, user= targetusername, password= targetpassword)
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

def pushToDB(targethostname, targetdatabase, targetusername, targetpassword, targetfile, data, TransformJob) :
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
				row = transformJobList(i,TransformJob[i],row)
			ar.append(row)
			print(row)	
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

def openCSV(targetfile, delim) :
	file = csv.reader(open(targetfile, "r"), delimiter = delim)
	return file

# def transformJob(file) :


def readColumn(targetfile):
	file = csv.reader(open(targetfile, "r"), delimiter = ',')
	col = []
	tipe = []
	ex = []
	i = 0
	for row in file :
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

	# print("col", col)
	# print("example", ex)
	# print("type", tipe)
	# print("len", len(col))
	return col, ex, tipe

def defineColumn(column, example):
	print("\n\nDefine column type: ")
	print("varchar/int/int8/date/timestamp ")
	kv = {}
	for i in range(len(column)) :
		inputcheck = False
		while inputcheck == False:
			inputkv= input("\n" + str(i+1) +". type for column: " + column[i] +"\nexample values: "+ example[i]+"\n")
			if inputkv == 'date' or inputkv == 'varchar' or inputkv == 'int8' or inputkv == 'int4' or inputkv == 'timestamp' :
				inputcheck = True
				kv[i] = inputkv
			else :
				print('\nPLEASE INSERT THE INPUT CORRECTLY !!!\n')
		# print(i)
	return kv

def createColumn(collen):
	col = []
	kv = []
	for i in range(collen) :
		column = input('\ninsert no.' + str(i+1) + ' column name:\n')
		column = (column.replace("-","_"))
		column = (column.replace(" ","_"))
		col.append(column)
		while True :
			inputkv= input(str(i+1) +". type for column: " + column +"\n")
			if inputkv == 'date' or inputkv == 'varchar' or inputkv == 'int8' or inputkv == 'int4' or inputkv == 'timestamp' :
				inputcheck = True
				kv.append(inputkv)
				break
			else :
				print('Please insert the right type (varchar/int/int8/date/timestamp)\n')
				continue
	return col, kv


def createTableQuery(column, kv, targettable):
	query = "CREATE TABLE %s (" % targettable
	for i in range(len(column)) :
		if i == len(column)-1:
			query = query + column[i] + " " + kv[i] + " " + "NULL" +" "
		else :
			query = query + column[i] +" " + kv[i] +" " + "NULL," +" "
	query = query + ");"
	print("query:", query)
	return query

def select_query(tablename) :
	query = "SELECT * from %s" % tablename

def truncateTableQuery(tablename):
	query = "TRUNCATE TABLE %s " % tablename
	return query

def queryTable(targethostname, targetdatabase, targetusername, targetpassword, targettable, query, queryname):
	queryTableError = True
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


def runSetConfiguration():
	defineTargetDB()
	confName = input('\nSave Configuration to: \n')
	cnf = open(confName,'w')
	cnf.write('targethostname=%s\ntargetdatabase=%s\ntargetusername=%s\ntargetpassword=%s\ntargettable=%s\n' % (targethostname,targetdatabase,targetusername,targetpassword,targettable))
	print('Saving file...')
	time.sleep(2)
	print('Done')
	time.sleep(2)

def runCreateNewTable():
	print('\nPlease select the input data')
	print('1: '+ SourceList[1])
	print('2: '+ SourceList[2])
	print('3: '+ SourceList[5])
	while True :
		try :
			selectedSource = int(input('\ninput:\n'))
		except:
			print('Please only input the number\n')
			continue
		else:
			if selectedSource in [1,2,3] :
				break
			else :
				print('Please insert the right value\n')
				continue

	if selectedSource == 1 :
		defineTargetDB()
		targetfile = input('\nInsert Path of targetfile:\n')
		column, example,_ = readColumn(targetfile)
		kv = defineColumn(column, example)
		query = createTableQuery(column, kv, targettable)
		result, queryCreateTableError = queryTable(targethostname, targetdatabase, targetusername, targetpassword,targettable, query, 'Create table')
		print(result)
	elif selectedSource == 3 :
		defineTargetDB()
		while True:
			try :
				collen = int(input('\nHow many column will be made\n'))
			except:
				print('Please only input the number\n')
				continue
			else : 
				break
		col, kv = createColumn(collen)
		query = createTableQuery(col, kv, targettable)
		result, queryCreateTableError = queryTable(targethostname, targetdatabase, targetusername, targetpassword,targettable, query, 'Create table')
		print(result)

def runInsertTable():
	print('\nPlease select the input data')
	print('1: '+ SourceList[1]) #LOCAL CSV
	print('2: '+ SourceList[2]) #LOCAL XLSX

	while True :
		try :
			selectedSource = int(input('\ninput:\n'))
		except:
			print('\nPlease only input the number\n')
			continue
		else:
			if selectedSource in [1,2] :
				break
			else :
				print('\nPlease insert the right value\n')
				continue
	print('\n\n1.Insert to existing table\n2.Insert to new table ')
	while True:
		try :
			selectedSubMenu = int(input('\ninput:\n'))
		except :
			print('Please only input the number\n')
			continue
		else:
			if selectedSubMenu in [1,2] :
				break
			else :
				print('\nPlease insert the right value(1 or 2)\n')
				continue

	if selectedSubMenu == 1 :
		if selectedSource == 1 :
			defineTargetDB()
			targetfile = input('\nInsert Path of targetfile:\n')
			while True :
				delim = input('\nSelect delimiter (, or ; or |)\n')
				if delim in [',',';','|']:
					break
				else:
					print('\nPlease insert the right value\n')
					continue
			file = openCSV(targetfile, delim)
			data = []
			for row in file :
				data.append(row)
			TransformJob = defineTransformJob(data)
			print('\nTransformJob: ', TransformJob)
			result, error = pushToDB(targethostname, targetdatabase, targetusername, targetpassword, targetfile, data, TransformJob)
			print('\nResult: ', result)










if __name__ == '__main__':

	os.system('clear')
	mainScreen()
	time.sleep(1)
	print('\nSelect Menu: (Please input the number)' )
	for i in menulist :
		print(str(i)+':', menulist[i])

	while True:
		try :
			selectedMenu = int(input('\ninput:\n'))
		except :
			print('Please only input the number\n')
			continue
		else:
			if selectedMenu in [1,2,3,4,5] :
				break
			else :
				print('Please insert the right value\n')
				continue

	if selectedMenu == 1 :
		runCreateNewTable()
	if selectedMenu == 2 :
		runInsertTable()
	if selectedMenu == 5 :
		runSetConfiguration()






	
	# l = open(logfile,'a')
	# # lf = open(logError, 'a')
	# log = "\n\n///////// Doing ETL - log -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "- Doing ETL //////////"
	# print(log)
	# l.write(log)
	# log = "log -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "source:" + targetfile
	# print(log)
	# l.write(log)
	# log = "log -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "target:" + targettable
	# print(log)
	# l.write(log)

	# resultGetSFTP, getFromSFTPError = getFromSFTP(sourcehostname, sourceport, sourceusername, sourcepassword, remoteFilePath, targetfile)
	# log = "log -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + resultGetSFTP
	# print(log)
	# l.write(log)

	# if getFromSFTPError == True :
		# l.write("get from sftp error")
		# lf.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + '///' + targetfile + '///' + targettable + '///' +  "get from sftp error")
	# 	exit()
	# else : 
	# 	pass

	# f = open('transformed_pac.csv', 'w')
	# with f:
	# 	file = csv.reader(open(targetfile, "r"), delimiter = ',')
	# 	next(file, None)
	# 	writer = csv.writer(f)
	# 	for row in file :
	# 		writer.writerow(transformJob_PAC(row))

	# resultpushToDB, pushToDBError = excelPushToDB(targethostname, targetdatabase, targetusername, targetpassword, targetfile)
	# log = "log -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + resultpushToDB
	# print(log)
	# l.write(log)

	# if pushToDBError == True :
	# 	l.write("push to DB is error")
	# 	lf.write(datetime.now().strftime("%d/%m/%Y %H:%M:%S") + '///' +targetfile + '///' + targettable + '///'  + "push to DB is error")
	# 	exit()
	# else : 
	# 	pass

	# log = "///////// ETL Finished - log -" + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "- ETL Finished ////////// \n"
	# print(log)
	# l.write(log)
	# l.close()


# file = csv.reader(open('contohcsv.csv', 'r'), delimiter = ',')
# x = 0
# for row in file :
# 	if x > 0 and x < 10 :
# 		column2.append(row[0])
# 	x += 1
	




