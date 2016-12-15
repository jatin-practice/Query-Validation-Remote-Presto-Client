'''
Created on 02-May-2015

@author: jatinrout
'''
from threading import Thread
from SqlExecution import *
class query_execution:
    def __init__(self):
        
        self.prestosql=PrestoSql('sql-file')
        self.prestosql.loadconfig('config.ini')
        self.serverIP=getattr(self.prestosql,'prestoserver')
        self.catalog=getattr(self.prestosql,'catalog')
        self.port=getattr(self.prestosql,'port')
        self.query_execution_mode=getattr(self.prestosql,'query_execution_mode')
        self.prestosql.populate_sqlfile()
        self.sqlfile=getattr(self.prestosql,'sql_file')
        
    #Function used for Sequential query execution
    
    def sequential_mode(self):
        self.prestosql.populate_sqlfile()
        self.prestosql.execute()
    #Method used for parallel query execution 
     
    def parallel_mode(self):
        self.prestosql1=PrestoSql('sql-file')
        self.prestosql2=PrestoSql('sql-file')
        self.prestosql3=PrestoSql('sql-file')
        
        self.prestosql1.loadconfig('config.ini')
        self.prestosql1.populate_sqlfile()
        self.prestosql2.loadconfig('config.ini')
        
        self.prestosql2.populate_sqlfile()
        self.prestosql3.loadconfig('config.ini')
        self.prestosql3.populate_sqlfile()
        
        
         
        threadlist=[]
        thread1=Thread(target=self.prestosql1.execute)
        thread2=Thread(target=self.prestosql2.execute)
        thread3=Thread(target=self.prestosql3.execute)
        
        threadlist.append(thread1)
        threadlist.append(thread2)
        threadlist.append(thread3)
        
        for thread in threadlist:
            thread.start()
            
if __name__=='__main__':
    prestosql=query_execution()
    
    if(prestosql.query_execution_mode=='sequential'):
        prestosql.sequential_mode()
    elif(prestosql.query_execution_mode=='parallel'):
        prestosql.parallel_mode()
    else:
        print "Invalid entry for execution mode" +'\n'+ 'It should be sequential or parallel'
    
          
        
        