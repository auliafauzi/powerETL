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
import json
import common 
from common import cnf,menulist,SourceList
import transform 
import connect


today = date.today()
cnf = common.cnf

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























def runCreateNewTable():
	cnf = common.cnf
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

	if selectedSource == 3 :
		print('\nConfigure target Database')
		print('1: From configuration file')
		print('2: Manual Configure')
		while True :
			try :
				selectedConf = int(input('\ninput:\n'))
			except:
				print('Please only input the number\n')
				continue
			else:
				if selectedConf == 1 :
					cnf = common.readConfiguration(cnf)
					cnf = common.defineTargetDB(3, cnf)
					break
				elif selectedConf == 2 :
					cnf = common.defineTargetDB(1, cnf)
					break
				else :
					print('Please insert the right value\n')
				continue		
		while True:
			try :
				collen = int(input('\nHow many column will be made\n'))
			except:
				print('Please only input the number\n')
				continue
			else : 
				break
		col, kv = common.createColumn(collen)
		query = connect.createTableQuery(col, kv, cnf['targetConf']['targettable'])
		result, queryCreateTableError = connect.queryTable(cnf, query, 'Create table')
		print(result)

	else :
		print('\nConfigure target Database')
		print('1: From configuration file')
		print('2: Manual Configure')
		while True :
			try :
				selectedConf = int(input('\ninput:\n'))
			except:
				print('Please only input the number\n')
				continue
			else:
				if selectedConf == 1 :
					cnf = common.readConfiguration(cnf)
					cnf = common.defineTargetDB(3, cnf)
					break
				elif selectedConf == 2 :
					cnf = common.defineTargetDB(1, cnf)
					break
				else :
					print('Please insert the right value\n')
					continue
		targetfile = input('\nInsert Path of targetfile:\n')
		if selectedSource == 1 :
			data, cnf = common.openCSV(targetfile,'')
		elif selectedSource == 2 :
			data = common.readExcel(targetfile)
		else : 
			pass
		result, queryCreateTableError = connect.createTablefunc(data, cnf)
		print(result)

def runInsertTable():
	cnf = common.cnf
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

	print('\nConfigure target Database')
	print('1: From configuration file')
	print('2: Manual Configure')
	while True :
		try :
			selectedConf = int(input('\ninput:\n'))
		except:
			print('Please only input the number\n')
			continue
		else:
			if selectedConf == 1 :
				cnf = common.readConfiguration(cnf)
				cnf = common.defineTargetDB(3, cnf)
				break
			elif selectedConf == 2 :
				cnf = common.defineTargetDB(1, cnf)
				break
			else :
				print('Please insert the right value\n')
				continue
	targetfile = input('\nInsert Path of targetfile:\n')
	if selectedSource == 1 :	
		data, cnf = common.openCSV(targetfile,'')			
	elif selectedSource == 2 :
		data = common.readExcel(targetfile)
	else :
		pass
	cnf['transformjob'] = transform.defineTransformJob(data, 'predict')
	print('\nTransformjob: ', cnf['transformjob'])
	time.sleep(1)
	queryCreateTableError = True
	if selectedSubMenu == 2 :
		result, queryCreateTableError = connect.createTablefunc(data)
		print(result)
	else :
		pass
	time.sleep(1)
	result, error = connect.pushToDB(cnf, targetfile, data, connect.insertQuery(cnf['targetConf']['targettable'], data))
	if error == True and queryCreateTableError == False :
		query = connect.queryTable(cnf, connect.dropTableQuery(targettable), 'DROP TABLE')
		print(query)
	print('\nResult: ', result)


def createETLProject():
	cnf = common.cnf
	# global projectName, fileType
	os.system('clear')
	cnf['common']['projectName'] = input('\nProject name:\n')
	cnf = common.defineTargetDB(1, cnf)
	cnf = common.defineSource(cnf)
	cnf = common.defineCommon(cnf)
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
		cnf['common']['fileType'] = 'csv'
		targetfile = input('\nPlease insert file name:\n')	
		data, cnf = common.openCSV(targetfile,'')			
	elif selectedSource == 2 :
		cnf['common']['fileType'] = 'xlsx'
		targetfile = input('\nPlease insert file name:\n')	
		data = common.readExcel(targetfile)
	else :
		pass
	transformJob = transform.defineTransformJob(data,'predict')
	print('\nTransformJob: ', transformJob)
	time.sleep(1)
	while True :
		if os.path.exists(cnf['common']['projectName']) == True :
			projectName = input('\nProject name: '+ cnf['common']['projectName'] +' is already exist, please insert different project name:\n')
			cnf['common']['projectName'] = projectName
			continue
		else :
			break
	os.mkdir(cnf['common']['projectName'])
	with open("oldlist", "w") as file:
		file.write('')
	errorCreateCnf =  common.createJobConfiguration(cnf, cnf['common']['projectName']+'/'+cnf['common']['projectName']+'.cnf', transformJob, cnf['common']['projectName']) # harusnya di akhir
	if errorCreateCnf == True :
		try :
			os.rmdir(cnf['common']['projectName'])
			print('Project Creation is failed...')
		except :
			pass
	common.createProjectJob(cnf['common']['projectName'])
	print('DONE...')
	time.sleep(1)









if __name__ == '__main__':

	os.system('clear')
	# print()
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
			if selectedMenu in [1,2,3,4,5,6] :
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
	if selectedMenu == 6 :
		readConfiguration()
	if selectedMenu == 3 : 
		createETLProject()






	
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
	




