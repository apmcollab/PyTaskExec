import sqlite3
import os
import string
import subprocess

class SqUtilities(object):
#
#===========     Begin Main Program   ============================
#
  @staticmethod
  def removeExistingTable(sqDB,runTableName):
    sqCommand = 'DROP TABLE ' + runTableName + ';'
    try:
      sqDB.execute(sqCommand)
    except sqlite3.OperationalError :
      pass
    
  @staticmethod
  def executeDB(sqDB,sqCommand):
    try :
      sqDB.execute(sqCommand)
    except sqlite3.OperationalError as e:
      print(e)
      exit(1)
      
  @staticmethod
  def getColumnValues(r,index):
    vals = []
    for i in r:
      vals.append(i[index])
    return vals
  
  @staticmethod
  def runProgram(command):
      p = subprocess.Popen(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      (r,e) = (p.stdout, p.stderr)
      sqError        = e.read();
      if(os.path.sep == '\\') : 
        sqError = sqError.replace("\r\n",'')
      else:                     
        sqError = sqError.replace("\n",'')
      if(sqError != ''): 
        print (sqError)
      r.close()
      e.close() 
  
  