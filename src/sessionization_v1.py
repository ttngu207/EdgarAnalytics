# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 17:42:41 2018

DON'T USE THIS ONE - PLEASE USE "sessionization.py"
THIS SCRIPT IS MEANT FOR TESTING OUT IDEAS ONLY (please ignore it)

Solving the Insight DataEngineering challenge 
SEC logfile sessionization


@author: TN
"""

'=============== Import packages ============='
import numpy as np
import csv 
import pandas as pd
import sessionizationSupports_TN
import time, timeit

'=============== Set directory and filename =============='

topdir = 'C:\\Users\\TN\\OneDrive\\Python Learning\\Edgar_Analytics\\edgar-analytics\\'
outputfile_name = 'sessionization.txt'
SEClog_dir = 'D:\\Python Scripts\\EdgarAnalytics\\'
SEC_logfile_name = 'SEC_log20170630.csv'

inactivity_periodfile = topdir + 'input\\' + 'inactivity_period.txt'

'=============== Do stuffs ================='

# Get the inactivity period
f = open(inactivity_periodfile)
inactivity_period = f.readline()
f.close()
inactivity_period = int(inactivity_period.split('\n')[0])

# Handle the SEC log line by line (as if the data is actually being streamed)
colnameDict = {'ip':0, 'date':1, 'time':2, 'zone':3, 'cik':4, 'accession':5, 'extention':6, 'code':7,
       'size':8, 'idx':9, 'norefer':10, 'noagent':11, 'find':12, 'crawler':13, 'browser':14}
itemOfInterest = [colnameDict['ip'],colnameDict['date'],colnameDict['time'],colnameDict['cik'],colnameDict['accession'] ]

' -------'
t_start = time.time()
f_output = open(topdir+'output\\'+outputfile_name,'w')
f_log = open(SEClog_dir+SEC_logfile_name)
dataReader = csv.reader(f_log)
count = 0

# Create an "AllOpenedSessions" object
allOpenedSessions = sessionizationSupports_TN.AllOpenedSession(f_output,inactivity_period)

for dataLine in dataReader:
    if dataReader.line_num == 1: continue
    
    '---- Processing ----'
    #print(np.array(dataLine)[itemOfInterest])
    
    ip = dataLine[colnameDict['ip']]
    accessDate = dataLine[colnameDict['date']]
    accessTime = dataLine[colnameDict['time']]
    
    allOpenedSessions.process_newline(ip, accessDate, accessTime)
    '---- End processing ----'
    
    count = count+1    
    if count == 200000: break    

'---- End of stream, set all current sessions to expire ----'
allOpenedSessions.all_sessions_expire()
del allOpenedSessions

f.close()  
t_end = time.time()  
print('Run time: %f second' % (t_end - t_start))
    
'=============== R&D ================='

CSVdata = pd.read_csv(SEClog_dir+SEC_logfile_name, nrows=20000)
CSVdata = CSVdata[['ip','date','time','cik','accession']]
CSVdata.tail()

D = pd.DataFrame(CSVdata['ip'].unique(),dtype=str)
D1 = D
D2 = D.astype('category')
D3 = np.array(D,dtype=str)

print(sys.getsizeof(D1))
print(sys.getsizeof(D2))
print(sys.getsizeof(D3))
























