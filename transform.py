
import re
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
import os
import decimal
import common
# import xlrd

def secondsToTime(n): 
    return str(timedelta(seconds = int(n)/10)) 

def excel_time_float(x):
	x = int(float(x) * 24 * 3600) # convert to number of seconds
	hour = x//3600
	minute = (x%3600)//60
	seconds = x%60
	return str(str(hour) + ':' + str(minute) + ':' + str(seconds))

# def excel_time_float(x) :
# 	return xlrd.xldate_as_tuple(x)  
      
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
	elif func == 'd-Mon-y H.M':
		row[n] = datetime.strptime(row[n], '%d-%b-%y %H.%M').strftime("%Y-%m-%d %H:%M:%S")
		return row
	elif func == 'seconds_integer_value' :
		row[n] = secondsToTime(row[n])
		return row
	elif func == 'excel_time_float':
		row[n] = excel_time_float(row[n])
		return row
	elif func == 'float4' or func == 'float8' :
		try :
			row[n] = '%.0f' % float(row[n])
		except :
			pass
		return row

def defineTransformJob(data,job) :
	global TransformJob
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
			inputkv= input('\nType for column ' + str(i+1) + ":\nColumn name: " + data[0][i] + '\nexample value: ' + data[1][i]+ "\n(OPTIONS : varchar/int4/int8/float4/float8/date/time)\ninput:\n")
			if inputkv == 'varchar' :
				break
			elif inputkv in ['int4', 'int8'] :
				excolumn = common.getSample(data, i)
				excolumn = ''.join(excolumn)
				chkResult = common.checkContainStr(excolumn)
				print("\n"+chkResult+ "\n")
				if chkResult == 'Contain Character' or chkResult == 'Contain Special Character' :
					choices = input("\nValue has non digit character, remove any non digit character and transform to integer? (y/n)\n")
					if choices == 'y' :
						TransformJob[i] = inputkv
						break
					else :
						continue
				elif chkResult == 'Contain Decimal Separator' :
					print('\nValue contain decimal separator, suggesting float for variable type')
					choices = input("\nRemove Decimal Separator and transform to integer? (y/n)\n")
					if choices == 'y' :
						TransformJob[i] = inputkv
						break
					else :
						continue
					continue
				else :
					TransformJob[i] = inputkv
					break
			elif inputkv in ['float4', 'float8'] :
				excolumn = common.getSample(data, i)
				excolumn = ''.join(excolumn)
				chkResult = common.checkContainStr(excolumn)
				print("\n"+chkResult+ "\n")
				if chkResult == 'Contain Character' or chkResult == 'Contain Special Character' :
					choices = input("\nValue has non digit character, remove any non digit character and transform to integer? (y/n)\n")
					if choices == 'y' :
						TransformJob[i] = inputkv
						break
					else :
						continue
				else :
					TransformJob[i] = inputkv
					break
			elif inputkv == 'date' and job == 'predict' :
				print('\nProcessing date value... Figuring what kind date type it is...')
				time.sleep(2)
				print('\nSample value : ', data[1][i])
				choices = defineDateType(data[1][i])
				if choices == 'special case' :
					column = common.getOneColumn(data, i)
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
			elif inputkv == 'date' :
				TransformJob[i] = inputkv
				break
			elif inputkv == 'd-Mon-y H.M' :
				TransformJob[i] = inputkv
				break
			elif inputkv == 'time' :
				print('\nSample value : ', data[1][i])
				print('What pattern that match value above ?')
				print('(OPTIONS : \n(1) HH:MM:SS \n(2) HH.MM.SS \n(3) seconds_integer_value \n(4) excel_time_float')
				while True:
					choices = input('\ninput:\n')
					if choices in ['1' , '2', '3', '4'] :
						if choices == '4' :
							print('sample value : ', excel_time_float(data[1][i]))
							time.sleep(1.5)
						break
					else :
						choices = ''
						print('\nPLEASE INSERT INPUT CORRECTLY !')
						print('(HH:MM:SS , HH.MM.SS, seconds_integer_value, excel_time_float)\n')
						continue
				if choices == '1' :
					TransformJob[i] = 'HH:MM:SS'
				elif choices == '2' :
					TransformJob[i] = 'HH.MM.SS'
				elif choices == '3' :
					TransformJob[i] = 'seconds_integer_value'
				elif choices == '4' : 
					TransformJob[i] = 'excel_time_float'
				break
			else :
				print('\nPLEASE INSERT INPUT CORRECTLY !\n')
				time.sleep(1.5)
				continue
	return TransformJob


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
