'''
Created on 11-May-2015

@author: jatinrout
'''
from SqlExecution import *
 
class datavalidation:
    def __init__(self):
        self.handle=PrestoSql('sql-file')
        self.fh_write=open('validation_query_results','w')
        
        self.logger = logging.getLogger("presto-query")
        self.logger.setLevel(logging.DEBUG)
        self.loghandle = logging.FileHandler('Validation_error_log')
        self.loghandle.setLevel(logging.INFO)
        
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.loghandle.setFormatter(formatter) 
        self.logger.addHandler(self.loghandle)   
        
    def load_config(self):
        self.handle.loadconfig('config.ini')
        self.serverIP=getattr(self.handle,'prestoserver')
        self.catalog=getattr(self.handle,'catalog')
        self.port=getattr(self.handle,'port')
        self.query_execution_mode=getattr(self.handle,'query_execution_mode')
        self.sqlfile=getattr(self.handle,'validationsql_file')
        self.results_file=getattr(self.handle,'result_file')
        
    def query_results(self):
        self.presto=PrestoClient(self.serverIP,self.port,self.catalog)
        lines = [line.strip() for line in open('%s'%(self.sqlfile))]
        for line in lines:
            if not self.presto.runquery(line):
                    
                    logs="Error"+self.presto.getlasterrormessage()+'----'+"Query"+'----'+str(line)
                    self.logger.error(logs)  
            else:
                #We're done now, so let's show the results
                #print 'Columns: ', presto.getcolumns()
                if self.presto.getdata():
                    #self.handle.presto.getnumberofdatarows()
                    
                    self.fh_write.write('%s\n'%(sorted(self.presto.getdata())))
                    
        self.fh_write.close()
                
        
        
if __name__=='__main__':
    
    handle=datavalidation()
    handle.load_config()
    handle.query_results()
    #handle.Compare()