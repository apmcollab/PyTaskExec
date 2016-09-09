import subprocess
import time
import random
import os

from pOpen2  import Popen
from pOpen2  import recv_some
from pOpen2  import send_all

class sqLiteError(Exception):
    def __init__(self, message=None):
        self.message = message
    def __repr__(self):
        return `self.message`

class sqLite(object):
  def __init__(self,runDBname):
    self.runDBname     = runDBname
    self.sqLiteCommand = 'sqlite3'
    self.maxTryCount   = 200
    self.tryTime       =.01
    self.verbose       = False
    self.isLocked      = False
    
    if os.sys.platform == 'win32':
        self.sqTime        = 0.1
    else:
        self.sqTime        = 0.5   # retry time for pOpen2 recv_some
                                   # is longer for a slow network
    #
    # For a Unix/Linux system find the sqlite3 command 
    #
    if os.sys.platform != 'win32':
      self.sqLiteCommand = subprocess.Popen('which sqlite3',shell=True,stdout=subprocess.PIPE).communicate()[0] 
      self.sqLiteCommand = self.sqLiteCommand.replace("\n",'')
      if(not os.path.isfile(self.sqLiteCommand)):
        message = 'SQlite command not Found.\n Search returned: ' + self.sqLiteCommand
        raise sqLiteError()
   
    if os.sys.platform == 'win32':
        self.tail = "\r\n"
    else:
        self.tail = "\n"
        
  def createConnection(self):
    PIPE    = subprocess.PIPE
    command = self.sqLiteCommand + ' ' + self.runDBname; # CR is not required
    if os.sys.platform == 'win32':
        self.sqProg  = Popen(command,stdin=PIPE,stdout=PIPE,stderr=PIPE)
    else:
        self.sqProg  = Popen(command,shell=True,stdin=PIPE,stderr=PIPE,stdout=PIPE)
    
  def execute(self,sqCommand):
    send_all(self.sqProg,sqCommand+self.tail)
    sqMessage = recv_some(self.sqProg,t=self.sqTime,e=1)
    if(sqMessage.find('SQL error') >= 0) :
      sqMessage = sqMessage.split(':')[1]
      if os.sys.platform == 'win32':
        sqMessage = sqMessage.replace('\r\n','')
      else:
        sqMessage = sqMessage.replace('\n','')
      eMessage = 'SQL error: ' + sqMessage
      raise sqLiteError(eMessage)
    else:
      return sqMessage
    
  def executeNoReturn(self,sqCommand):
    send_all(self.sqProg,sqCommand+self.tail)
    sqMessage  = self.sqProg.recv()
    time.sleep(self.sqTime)
    if(sqMessage):
      if(sqMessage.find('SQL error') >= 0) :
        sqMessage = sqMessage.split(':')[1]
        if os.sys.platform == 'win32':
          sqMessage = sqMessage.replace('\r\n','')
        else:
          sqMessage = sqMessage.replace('\n','')
        eMessage = 'SQL error: ' + sqMessage
        raise sqLiteError(eMessage)
    
  def executeWithRepeat(self,sqCommand):
    if(self.isLocked):
          message = 'SQL Error: The database \"' \
            + self.runDBname + '\" is locked by this process.'
          raise sqLiteError(message)
    tryCount     = 0
    openLockFail = True
    retryFlag    = False
    while((tryCount < self.maxTryCount)and(openLockFail)):
          send_all(self.sqProg,sqCommand+self.tail)
          sqMessage = recv_some(self.sqProg,t=self.sqTime,e=1)
          if(sqMessage.find('database is locked') >= 0):
              retryFlag = True 
              if(self.verbose): print 'Database locked: retrying ...'
              time.sleep(self.tryTime*random.random())
              tryCount = tryCount + 1
          else:
              openLockFail = False
          if(tryCount >= self.maxTryCount):
            message = 'SQL Error:  Unable to obtain a lock.\n\
            The database \"' + self.runDBname + '\" is locked\n\
            by another process.'
            raise sqLiteError(message)
    if(retryFlag and self.verbose): print 'Database success'
    return sqMessage
  
  def executeWithRepeatNoReturn(self,sqCommand):
    if(self.isLocked):
          message = 'SQL Error: The database \"' \
            + self.runDBname + '\" is locked by this process.'
          raise sqLiteError(message)
    tryCount     = 0
    openLockFail = True
    retryFlag    = False
    while((tryCount < self.maxTryCount)and(openLockFail)):
          send_all(self.sqProg,sqCommand+self.tail)
          time.sleep(self.sqTime)
          sqMessage  = self.sqProg.recv()
          if(sqMessage.find('database is locked') >= 0):
              retryFlag = True 
              if(self.verbose): print 'Database locked: retrying ...'
              time.sleep(self.tryTime*random.random())
              tryCount = tryCount + 1
          else:
              openLockFail = False
          if(tryCount >= self.maxTryCount):
            message = 'SQL Error:  Unable to obtain a lock.\n\
            The database \"' + self.runDBname + '\" is locked\n\
            by another process.'
            raise sqLiteError(message)
    if(retryFlag and self.verbose): print 'Database success'
    return sqMessage
  
  def executeWithLock(self,sqCommand):
    self.getLock()
    sqReturn = self.execute(sqCommand)
    self.clearLock()
    return sqReturn
    
  def getLock(self):
    if(self.isLocked):
          message = 'SQL Error: The database \"' \
            + self.runDBname + '\" is already locked by this process.'
          raise sqLiteError(message)
    tryCount     = 0
    openLockFail = True
    retryFlag    = False
    while((tryCount < self.maxTryCount)and(openLockFail)):
          sqMessage = ''
          send_all(self.sqProg,'BEGIN EXCLUSIVE;' + self.tail)
          time.sleep(self.sqTime)
          sqMessage  = self.sqProg.recv()
          if(sqMessage.find('database is locked') >= 0):
              retryFlag = True 
              if(self.verbose): print 'Database locked: retrying ...'
              time.sleep(self.tryTime*random.random())
              tryCount = tryCount + 1
          else:
              openLockFail = False
          if(tryCount >= self.maxTryCount):
            message = 'SQL Error:  Unable to obtain a lock.\n\
            The database \"' + self.runDBname + '\" is locked\n\
            by another process.'
            raise sqLiteError(message)
    if(retryFlag and self.verbose): print 'Lock obtained'
    self.isLocked = True
    
  def clearLock(self):
    if(self.isLocked):
      self.executeNoReturn('END;' + self.tail)
      self.isLocked = False
    else:
      self.isLocked = False
   
  def closeConnection(self):
    send_all(self.sqProg,'.quit' + self.tail)
  
  def insertVal(val,valType):
     if(valType == 'TEXT'):    return  "\'" + val + "\'"
     if(valType == 'INTEGER'): return '%d'      % val
     if(valType == 'REAL'):    return '%-.16e'  % val
     if(valType == 'BLOB'):    return val
     
  def getSQLiteType(val):
     if(type(val) is int    ): return 'INTEGER'
     if(type(val) is str    ): return 'TEXT' 
     if(type(val) is float  ): return 'REAL'
     return 'BLOB'
 
  def getSQLiteValue(val):
     if(type(val) is int    ): return '%d'      % val
     if(type(val) is str    ): return  "\'" + val + "\'" 
     if(type(val) is float  ): return '%-.16e'  % val
     return val
      
  insertVal       = staticmethod(insertVal)
  getSQLiteType  = staticmethod(getSQLiteType)
  getSQLiteValue = staticmethod(getSQLiteType)
  

    
   

