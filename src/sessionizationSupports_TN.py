# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 17:42:41 2018

A simple module to assist SEC sessionization procedure
Creating 2 classes
    - OpenedSession
    - AllOpenedSession

@author: TN
"""

import numpy as np
import datetime 

class OpendedSession:
    
    '---- Some attributes ----'
    IP = str()
    startDateTime = datetime.datetime
    numDocRequested = 0
    lastRequestTime = datetime.datetime
    inactivity_period = int()
    
    __date_formatting = '%Y-%m-%d'
    __time_formatting = '%H:%M:%S'
    __datetime_formatting = __date_formatting + ' ' + __time_formatting
    
    def __init__(self, ip, startdate, starttime, inactivity_period=1):
        self.IP = ip
        self.startDateTime = datetime.datetime.strptime(startdate + ' ' + starttime, self.__datetime_formatting)
        self.lastRequestTime = self.startDateTime
        self.inactivity_period = inactivity_period
        self.numDocRequested = self.numDocRequested + 1
        #print('Create new session - IP: ' + ip)
        
    def is_session_over (self,current_datetime):
        '''
        If the time elapsed between the current time and the last request time 
        is more than the specified "inactivity period", then this session is finished
        '''
        if not(isinstance(current_datetime,datetime.datetime)):
            current_datetime = datetime.datetime.strptime(current_datetime, self.__datetime_formatting)

        elapsedTime = current_datetime - self.lastRequestTime
        if elapsedTime.seconds > self.inactivity_period:
            return True
        else:
            return False
        
    def add_new_request (self,current_datetime):
        '''
        A new document request is made by this IP
        Thus, the number of documents requested increase by 1
        Update the "last request time" to "now"
        '''
        if not(isinstance(current_datetime,datetime.datetime)):
            current_datetime = datetime.datetime.strptime(current_datetime, self.__datetime_formatting)
        
        self.numDocRequested = self.numDocRequested + 1
        self.lastRequestTime = current_datetime
        
    def generate_ending_report(self):
        sessionDuration = self.lastRequestTime - self.startDateTime
        endingReport = self.IP + ',' + \
                        self.startDateTime.strftime(self.__datetime_formatting) + ',' + \
                        self.lastRequestTime.strftime(self.__datetime_formatting) + ',' + \
                        str(sessionDuration.seconds) + ',' + \
                        str(self.numDocRequested)
        #print(endingReport) 
        return endingReport       
        
    
class AllOpenedSession:
    
    '---- Some attributes ----'
    IPs = np.empty(0,dtype=str)      
    Sessions = np.empty(0,dtype=OpendedSession)       
    numSessions = 0
    inactivity_period = 1
    endingReports = list()

    __date_formatting = '%Y-%m-%d'
    __time_formatting = '%H:%M:%S'
    __datetime_formatting = __date_formatting + ' ' + __time_formatting

    def __init__(self, f_output, inactivity_period):
        self.inactivity_period = inactivity_period
        self.f_output = f_output
        
    def process_newline(self, ip, accessDate, accessTime):
        current_datetime =  datetime.datetime.strptime(accessDate + ' ' + accessTime, self.__datetime_formatting)
 
        '---- Check if this is the first line to process ----'
        if self.numSessions == 0:
            self.create_new_session(ip,accessDate,accessTime)
            return
        
        '---- Check if this IP is new or part of the previously opened session ----'
        intersectMask = np.in1d( self.IPs, np.array(ip) )
        intersectIndex = np.arange(self.numSessions)[intersectMask]
        
        '---- If existed, add a request to the corresponding session ----'
        if intersectIndex.size == 1:
            sess2update = self.Sessions[intersectIndex[0]]
            sess2update.add_new_request(current_datetime)
            self.Sessions[intersectIndex[0]] = sess2update 
            del sess2update
        elif intersectIndex.size == 0:  
            '---- If new, create a new session and add to sessions list ----'
            self.create_new_session(ip,accessDate,accessTime)               
            
        '---- Check all currently opened sessions to see if any expire ----'
        expiredSessions = np.zeros(self.numSessions, dtype = bool)
        for sessIndex, sess in enumerate(self.Sessions):
            expirestatus = sess.is_session_over(current_datetime)
            expiredSessions[sessIndex] = expirestatus
            # If expire, generate the session report
            if expirestatus:
                endingReport = sess.generate_ending_report()
                #print(endingReport)
                self.f_output.mode = 'a'
                self.f_output.write('%s\n' % endingReport)   
                #self.endingReports.append(endingReport)
              
        '---- Remove expired sessions out of the list of current opened sessions ----'
        notExpiredSessions = np.logical_not(expiredSessions)
        self.IPs = self.IPs[notExpiredSessions]
        self.Sessions = self.Sessions[notExpiredSessions]
        self.numSessions = self.numSessions - expiredSessions.nonzero()[0].size
        
        return
        
    def create_new_session(self,ip,accessDate,accessTime):
            newSess = OpendedSession(ip,accessDate,accessTime,self.inactivity_period)
            self.IPs = np.append(self.IPs,ip)
            self.Sessions = np.append(self.Sessions,newSess)
            self.numSessions = self.numSessions + 1
            del newSess
        
    def all_sessions_expire(self):
            for sessIndex, sess in enumerate(self.Sessions):
                    endingReport = sess.generate_ending_report()
                    #print(endingReport)
                    self.f_output.mode = 'a'
                    self.f_output.write('%s\n' % endingReport)   
                    #self.endingReports.append(endingReport)
        
        
        
        
        
        
        
        
        
        