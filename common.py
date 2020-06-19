
import csv
import re
import xlrd
import time
import os
from getpass import getpass
import decimal
import json
# import ETL_MENU

query = ""


cnf = {}
cnf['targetConf'] = {}
cnf['sourceConf'] = {}
cnf['common'] = {}
cnf['transformJob'] = {}
cnf['targetConf']['targethostname'] = ""
cnf['targetConf']['targetdatabase'] = ""
cnf['targetConf']['targetusername'] = ""
cnf['targetConf']['targetpassword'] = ""
cnf['targetConf']['targettable'] = ""
cnf['sourceConf']['sourcehostname'] = ""
cnf['sourceConf']['sourceport'] = ""
cnf['sourceConf']['sourceusername'] = ""
cnf['sourceConf']['sourcepassword'] = ""
cnf['sourceConf']['sourcelocation'] = ""
cnf['sourceConf']['sourcenamepatern'] = ""
cnf['common']['logfile'] = ""
cnf['common']['userEmail'] = ""
cnf['common']['developerEmail'] = ""
# cnf['transformJob'] = ""
cnf['common']['projectName'] = ""
cnf['common']['fileType'] = ""
cnf['common']['delimiter'] = ""

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



def defineTargetDB(select,cnf):
	# global targethostname, targetdatabase, targetusername, targetpassword, targettable
	os.system('clear')
	print('\nConfigure Target Database....\n')
	time.sleep(1)
	if select == 1 or select == 2 :
		cnf['targetConf']['targethostname'] = input('\nInsert target Host:\n')
		cnf['targetConf']['targetdatabase'] = input('\nInsert target database name:\n')
		cnf['targetConf']['targetusername'] = input('\nInsert username:\n')
		cnf['targetConf']['targetpassword'] = getpass('\nInsert password:\n')
	if select == 1 or select == 3 :
		cnf['targetConf']['targettable'] = input('\nInsert target table name \n(format : schema.tablename)\n(example : ods.oc_jurnal):\n')
	else : 
		pass
	return cnf

def defineSource(cnf) :
	# global sourcehostname, sourceusername, sourcepassword, sourceport, sourcelocation, sourcenamepatern
	os.system('clear')
	print('\nConfigure Source....\n')
	time.sleep(1)
	cnf['sourceConf']['sourcehostname']  = input('\nInsert source Host:\n')
	cnf['sourceConf']['sourceport'] = input('\nInsert target Port:\n')
	cnf['sourceConf']['sourceusername'] = input('\nInsert Username:\n') 
	cnf['sourceConf']['sourcepassword'] = getpass('\nInsert password:\n')
	cnf['sourceConf']['sourcelocation'] = input('\nInsert source location:\n') 
	cnf['sourceConf']['sourcenamepatern'] = input('\nInsert source name pattern:\n')
	return cnf

def defineCommon(cnf) :
	# global logfile, transformJob_List, userEmail, developerEmail
	cnf['common']['logfile'] = input('\nInsert source logfile name:\n')
	cnf['common']['userEmail'] = input('\nInsert user email address:\n')
	cnf['common']['developerEmail'] = input('\nInsert developer email address:\n')
	return cnf

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

def checkContainStr(string) :
	if re.findall("[a-zA-z]", string) != [] :
		return 'Contain Character'
	elif re.findall("[@_!#$%^&*()<>-?/\|}{~:]", string) != [] :
		return 'Contain Special Character'
	elif re.findall("[.,]", string) != [] :
		return 'Contain Decimal Separator'
	else :
		return 'Digit only'

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

def writeCSV(data) :
	with open('Output.csv', 'w', newline='') as myfile:
		wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
		for row in data :
			wr.writerow(row)

def openCSV(targetfile, delim) :
	global delimiter
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
	cnf['common']['delimiter'] = delim
	file = csv.reader(open(targetfile, "r"), delimiter = delim)
	data = []
	for row in file :
		data.append(row)
	return data, cnf


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

def defineColumn(column, example):
	print("\n\nDefine column type: ")
	print("varchar/int4/int8/float4/float8/date/time")
	kv = {}
	for i in range(len(column)) :
		inputcheck = False
		while inputcheck == False:
			inputkv= input("\n" + str(i+1) +". type for column: " + column[i] +"\nexample values: "+ example[i]+"\n")
			if inputkv == 'date' or inputkv == 'varchar' or inputkv == 'int8' or inputkv == 'int4' or inputkv == 'time' or inputkv == 'float4' or inputkv == 'float8' :
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

def runSetConfiguration():
	defineTargetDB(2)
	confName = input('\nSave Configuration to: \n')
	cnf = {}
	cnf['targethostname'] = targethostname
	cnf['targetdatabase'] = targetdatabase
	cnf['targetusername'] = targetusername
	cnf['targetpassword'] = targetpassword
	with open(confName,'w+') as outfile :
		json.dump(cnf,outfile)
	print('Saving file...')
	time.sleep(2)
	print('Done')
	time.sleep(2)

def createJobConfiguration(cnf, confName, transformJob, projectName):
	# defineTargetDB(2)
	try :
		# cnf = {}
		# cnf['targetConf'] = {}
		# cnf['sourceConf'] = {}
		# cnf['common'] = {}
		# cnf['transformJob'] = {}
		# cnf['targetConf']['targethostname'] = targethostname
		# cnf['targetConf']['targetdatabase'] = targetdatabase
		# cnf['targetConf']['targetusername'] = targetusername
		# cnf['targetConf']['targetpassword'] = targetpassword
		# cnf['targetConf']['targettable'] = targettable
		# cnf['sourceConf']['sourcehostname'] = sourcehostname
		# cnf['sourceConf']['sourceport'] = sourceport
		# cnf['sourceConf']['sourceusername'] = sourceusername
		# cnf['sourceConf']['sourcepassword'] = sourcepassword
		# cnf['sourceConf']['sourcelocation'] = sourcelocation
		# cnf['sourceConf']['sourcenamepatern'] = sourcenamepatern
		# cnf['common']['logfile'] = logfile
		# cnf['common']['userEmail'] = userEmail
		# cnf['common']['developerEmail'] = developerEmail
		cnf['transformJob'] = transformJob
		# cnf['common']['projectName'] = projectName
		# cnf['common']['fileType'] = fileType
		# cnf['common']['delimiter'] = delimiter
		with open(confName,'w+') as outfile :
			json.dump(cnf,outfile)
		print('Saving file...')
		time.sleep(2)
		print('Done')
		time.sleep(1)
		os.system('clear')
		error = False
		return error
	except :
		error = True
		return error

def readConfiguration(cnf):
	# global targethostname, targetdatabase, targetusername, targetpassword
	while True :
		confName = input('\nLoad Configuration file in: \n')
		try :
			with open(confName) as json_file:
			    cnf_load = json.load(json_file)
			    # print(cnf_load['targethostname'])
			    cnf['targetConf']['targethostname'] = cnf_load['targethostname']
			    cnf['targetConf']['targetdatabase'] = cnf_load['targetdatabase']
			    cnf['targetConf']['targetusername'] = cnf_load['targetusername'] 
			    cnf['targetConf']['targetpassword'] = cnf_load['targetpassword']
			break
		except : 
			print('failed to load the file, please check the filename or the configuration file content')
			time.sleep(1)
			continue
	os.system('clear')
	print('Load the configuration....\n')
	print(cnf['targetConf']['targethostname'])
	print(cnf['targetConf']['targetdatabase'])
	print(cnf['targetConf']['targetusername'])
	time.sleep(2)
	return cnf

def readConfigurationGUI(cnf, confName):
	# global targethostname, targetdatabase, targetusername, targetpassword
	while True :
		# confName = input('\nLoad Configuration file in: \n')
		try :
			with open(confName) as json_file:
			    cnf_load = json.load(json_file)
			    # print(cnf_load['targethostname'])
			    cnf['targetConf']['targethostname'] = cnf_load['targethostname']
			    cnf['targetConf']['targetdatabase'] = cnf_load['targetdatabase']
			    cnf['targetConf']['targetusername'] = cnf_load['targetusername'] 
			    cnf['targetConf']['targetpassword'] = cnf_load['targetpassword']
			break
		except : 
			print('failed to load the file, please check the filename or the configuration file content')
			time.sleep(1)
			continue
	os.system('clear')
	print('Load the configuration....\n')
	print(cnf['targetConf']['targethostname'])
	print(cnf['targetConf']['targetdatabase'])
	print(cnf['targetConf']['targetusername'])
	time.sleep(2)
	return cnf

def readGUIConf(confName):
	try :
		with open(confName) as json_file :
			gui_conf = json.load(json_file)
	except : 
			print('failed to load the file, please check the filename or the configuration file content')
			time.sleep(1)
	return gui_conf

def readJobConfiguration(confName):
	# global targethostname, targetdatabase, targetusername, targetpassword, sourcehostname,sourceport,sourceusername,sourcepassword,sourcelocation,sourcenamepatern,logfile,transformJob_List,userEmail,developerEmail,projectName,fileType,delimiter
	while  True :
		try :
			with open(confName) as json_file:
			    cnf_load = json.load(json_file)
			    cnf['targetConf']['targethostname'] = cnf_load['targetConf']['targethostname']
			    cnf['targetConf']['targetdatabase'] = cnf_load['targetConf']['targetdatabase']
			    cnf['targetConf']['targetusername'] = cnf_load['targetConf']['targetusername']
			    cnf['targetConf']['targetpassword'] = cnf_load['targetConf']['targetpassword']

			    cnf['sourceConf']['sourcehostname'] = cnf_load['sourceConf']['sourcehostname']
			    cnf['sourceConf']['sourceport'] = cnf_load['sourceConf']['sourceport']			    
			    cnf['sourceConf']['sourceusername'] = cnf_load['sourceConf']['sourceusername'] 
			    cnf['sourceConf']['sourcepassword'] = cnf_load['sourceConf']['sourcepassword']
			    cnf['sourceConf']['sourcelocation'] = cnf_load['sourceConf']['sourcelocation']
			    cnf['sourceConf']['sourcenamepatern'] = cnf_load['sourceConf']['sourcenamepatern']

			    cnf['common']['projectName'] = cnf_load['common']['projectName']
			    cnf['common']['logfile'] = cnf_load['common']['logfile']
			    cnf['transformJob'] = cnf_load['transformJob']
			    cnf['common']['userEmail'] = cnf_load['common']['userEmail']
			    cnf['common']['developerEmail'] = cnf_load['common']['developerEmail']
			    cnf['common']['fileType'] = cnf_load['common']['fileType']
			    cnf['common']['delimiter'] = cnf_load['common']['delimiter']
			break
		except : 
			print('failed to load the file, please check either the file is exist')
			continue
	os.system('clear')
	print('Load the configuration....\n')
	print(targethostname)
	print(targetdatabase)
	print(targetusername)
	time.sleep(2)
	return cnf


def createProjectJob(projectName) :
	a = "confName = '" +projectName+".cnf'\n\n"
	with open('job_general.py', 'r') as r :
		job = r.read()
	with open(projectName+'/'+projectName+'.py', 'a') as f :
		f.write(a)
		f.write(job)
	with open(projectName+'/'+'oldlist', 'w') as oldlist :
		oldlist.write('')
	with open(projectName+'/'+projectName+'_log.log', 'w') as log :
		log.write('')
