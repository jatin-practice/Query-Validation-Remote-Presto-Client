'''
Created on 26-Apr-2015

@author: jatinrout
'''
from prestoclient import getpass,httplib,json,PrestoClient,sleep,socket,urllib2
import ConfigParser
import time
import threading
import logging 
from pprint import pprint

class PrestoSql:
    def __init__(self,filename):
        self.filename=filename
        self.logger = logging.getLogger("presto-query")
        self.logger.setLevel(logging.DEBUG)
        self.loghandle = logging.FileHandler('presto_query_log')
        self.loghandle.setLevel(logging.INFO)
        
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.loghandle.setFormatter(formatter) 
        self.logger.addHandler(self.loghandle)     
        
        
    def loadconfig(self,config_file):
        
        self.logger.info('Loading the Config file')
        config=ConfigParser.ConfigParser()
        config.read(config_file)
        self.prestoserver=config.get('Presto','Presto_Server')
        self.catalog=config.get('Presto','catalog')
        self.schema=config.get('Presto','schema')
        self.port=config.get('Presto','inport')
        self.query_execution_mode=config.get('Presto','query_execution_mode')
        self.logfile=config.get('Presto','logfile_path')
        self.validationsql_file=config.get('Validation','Query_file')
        self.result_file=config.get('Validation','Results_file')
                
    ''' 
    This method will read the application.log and will parse the select queries 
    After parsing the file , it will populate the sql_file for next processing
    '''
         
    def populate_sqlfile(self):
        import re
        self.logger.info("Wait populating the SQL query file, will take some secs")
        self.sql_file='sql_query'
        FD_write=open(self.sql_file,'w')
        
        lines=[line.strip() for line in open('%s'%(self.logfile))]
        for line in lines:    
            
            matchobj=re.findall(r'select.*',line,re.M|re.I)
            
            if(matchobj):
                if('failed' in matchobj[0]):
                    pattern=matchobj[0].split(';')[0]
                #FD_write.write(matchobj.group(2)+'\n')
                else:
                    pattern=matchobj[0]
                FD_write.write(pattern+'\n')
            else:
                next
        

            
    def execute(self):
        self.presto=PrestoClient(self.prestoserver,self.port,self.catalog)
        count=1
        
        lock=threading.Lock()
        lines = [line.strip() for line in open('%s'%(self.sql_file))]
        #Recursively running the queries 
        while count<=1000:
            counter=1
            for line in lines:
                in_sql_statement=line
                millisecs=time.time()*1000 
                lock.acquire()
                if not self.presto.runquery(in_sql_statement):
                    
                    logs="Error"+self.presto.getlasterrormessage()+'----'+"Query"+'----'+in_sql_statement
                    self.logger.error(logs)  
                else:
                    #We're done now, so let's show the results
                    resposnsetime=time.time()*1000 - millisecs
                    
                    logs="ResponseTime in millisecs"+'----'+str(resposnsetime)+'----'+"Query"+'----'+in_sql_statement
                    self.logger.info(logs)
                    
                lock.release()
                 
                counter=counter+1
            count=count+1
                                        
                