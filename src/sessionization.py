# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

'=============== Import packages ============='
import sys
import numpy as np
import csv 
import datetime

'=============== Set directory and filename =============='

SEC_logfile_name = sys.argv[1]
inactivity_periodfile = sys.argv[2]
outputfile_name = sys.argv[3]



#%%
'=============== Set up some parameters ================='
# Get the inactivity period
f = open(inactivity_periodfile)
inactivity_period = f.readline()
f.close()
inactivity_period = int(inactivity_period.split('\n')[0])
# Set date-time formatting (ideally grab from a config file)
global __dateFormat__, __timeFormat__, __datetimeFormat__
__dateFormat__ = '%Y-%m-%d'
__timeFormat__ = '%H:%M:%S'
__datetimeFormat__ = __dateFormat__ + ' ' + __timeFormat__

# Some filtering param for the log file
colnameDict = {'ip':0, 'date':1, 'time':2, 'zone':3, 'cik':4, 'accession':5, 'extention':6, 'code':7,
       'size':8, 'idx':9, 'norefer':10, 'noagent':11, 'find':12, 'crawler':13, 'browser':14}
itemOfInterest = [colnameDict['ip'],colnameDict['date'],colnameDict['time'],colnameDict['cik'],colnameDict['accession'] ]


#%% 
'=============== Supporting functions ================='
def create_new_session(IPs, StartDateTime, numDocRequested, LastRequestTime, ip, current_datetime):
        new_IPs = np.append(IPs,ip)
        new_StartDateTime = np.append(StartDateTime,current_datetime)
        new_numDocRequested = np.append(numDocRequested,1)
        new_LastRequestTime = np.append(LastRequestTime,current_datetime)
        return new_IPs, new_StartDateTime, new_numDocRequested, new_LastRequestTime
        
def remove_expired_session(IPs, StartDateTime, numDocRequested, LastRequestTime, expiredSessionsMask):
        notExpiredMask = np.logical_not(expiredSessionsMask)
        new_IPs = IPs[notExpiredMask]
        new_StartDateTime = StartDateTime[notExpiredMask]
        new_numDocRequested = numDocRequested[notExpiredMask]
        new_LastRequestTime = LastRequestTime[notExpiredMask]
        return new_IPs, new_StartDateTime, new_numDocRequested, new_LastRequestTime

def generate_ending_report(ip, startDateTime, lastRequestTime, numDocRequested):
        sessionDuration = lastRequestTime - startDateTime + datetime.timedelta(seconds=1)
        endingReport = ip + ',' + \
                        startDateTime.strftime(__datetimeFormat__) + ',' + \
                        lastRequestTime.strftime(__datetimeFormat__) + ',' + \
                        str(sessionDuration.seconds) + ',' + \
                        str(numDocRequested)
        return endingReport       
       
#%%
    
' ==================== The heavy lifting ===================='
f_output = open(outputfile_name,'w')
f_log = open(SEC_logfile_name)
dataReader = csv.reader(f_log)
count = 0

# Create some arrays
IPs = np.ndarray(0,dtype=str)
StartDateTime = np.ndarray(0,dtype=datetime.datetime)
numDocRequested = np.ndarray(0,dtype=int)
LastRequestTime = np.ndarray(0,dtype=datetime.datetime)

f_output.mode = 'a'
for dataLine in dataReader:
    if dataReader.line_num == 1: continue
    
    '====== Processing ======'      
    ip = dataLine[colnameDict['ip']]
    accessDate = dataLine[colnameDict['date']]
    accessTime = dataLine[colnameDict['time']]
    current_datetime = accessDate + ' ' + accessTime
    
    current_datetime = datetime.datetime.strptime(current_datetime, __datetimeFormat__)

    '---- Very first line being stream?? ----'
    if IPs.size == 0:
        IPs, StartDateTime, numDocRequested, LastRequestTime = create_new_session(IPs, StartDateTime, numDocRequested, LastRequestTime, ip, current_datetime)
        continue
        
    '---- Check if this IP is new or part of the previously opened session ----'
    intersectMask = np.in1d( IPs, np.array(ip) )
    intersectIndex = np.arange(IPs.size)[intersectMask]
    
    '---- If existed, add a request to the corresponding session ----'
    if intersectIndex.size == 1:
        LastRequestTime[intersectIndex[0]] = current_datetime
        numDocRequested[intersectIndex[0]] = numDocRequested[intersectIndex[0]] + 1
    elif intersectIndex.size == 0:  
        '---- If new, create a new session and add to sessions list ----'
        IPs, StartDateTime, numDocRequested, LastRequestTime = create_new_session(IPs, StartDateTime, numDocRequested, LastRequestTime, ip, current_datetime)

    '---- Check all currently opened sessions to see if any expire ----'
    expiredSessionsMask = np.zeros(IPs.size, dtype = bool)
    for sessIndex, sess_lastRequestTime in enumerate(LastRequestTime):
        elapsedTime = current_datetime - sess_lastRequestTime
        expirestatus = elapsedTime.seconds > inactivity_period
        expiredSessionsMask[sessIndex] = expirestatus
        '-- If expire, generate the session report --'
        if expirestatus:
            endingReport = generate_ending_report(IPs[sessIndex], StartDateTime[sessIndex], LastRequestTime[sessIndex], numDocRequested[sessIndex])
            f_output.write('%s\n' % endingReport)   
    
    '---- Remove expired sessions out of the list of current opened sessions ----'
    IPs, StartDateTime, numDocRequested, LastRequestTime = remove_expired_session(IPs, StartDateTime, numDocRequested, LastRequestTime, expiredSessionsMask)
    
    '====== End processing ======'

'====== End of stream, set all current sessions to expire ======'
for k in list(range(0,IPs.size)):
    endingReport = generate_ending_report(IPs[k], StartDateTime[k], LastRequestTime[k], numDocRequested[k])
    f_output.write('%s\n' % endingReport)   

del IPs, StartDateTime, numDocRequested, LastRequestTime 

f.close()  
' ==================== Finished ===================='
 #%%




























