import os
from sqLite import sqLite
from sqLite import sqLiteError
from string import Template

import time
import datetime
import sqlite3

def getFullPath(fileName):
   if(os.path.isfile(fileName)):
       return os.path.abspath(fileName)
   else:
      print '                 === Error ==='
      print " Template file not found" 
      print exception
      exit() 
      
    
def testRunDataFile(runTemplateFile,runData):
  #
  # 
  try :
    f = open(runTemplateFile,'rU')
  except IOError,exception:
      print '                 === Error ==='
      print " Template file cannot be read" 
      print exception
      exit()
      
  runFileContents =  f.read()
  f.close()
  #
  # Verify that the template file is of Unix format 
  # (using construct from crlf.py in Toos/sripts)
  #
  if(runFileContents.find("\r\n")):
       newContents = runFileContents.replace("\r\n", "\n")
       f = open(runTemplateFile, "wb+")
       f.write(newContents)
       f.close()
  #
  # Check to make sure all of the parameters that are to be
  # substituted have template entries
  #
  runKeys = runData.keys()
  for i in range(len(runData)):
      findString = '$' + runKeys[i]
      findString = findString.strip()
      if(runFileContents.find(findString) == -1): 
          print '                 === Error ==='
          print ' Template variable \'' + findString +\
                '\' is not in the template file'
          print ' ' + runTemplateFile
          exit()
  #
  # Substitute in the parameters
  #
  r  = Template(runFileContents)
  try:
   runDataFile = r.substitute(runData)
  except KeyError, exception:
   print '                 === Error ==='
   print 'A parameter in: ' + runTemplateFile
   print 'has not been specified.\n'
   print 'The parameter that needs to be specified :'
   print 'runParams[',exception, ']'
   exit()
  

def checkForExistingDatabase(sqDB,runDBname,runTableName,noCheck):
# Checks db for existing task table and removes if desired.
  
# The database file is automatically created when the 
# data base connection is open
  if(noCheck):
        sqCommand = 'DROP TABLE ' + runTableName + ';'
        try:
           sqDB.executeNoReturn(sqCommand)
        except sqLiteError:
           pass
        return
       
  runDBname = os.path.basename(runDBname)
  sqCommand = 'SELECT ROWID from ' + runTableName + ' where ROWID = 1;'
  try:
     sqDB.execute(sqCommand)
     print '>>> The task database '  + runDBname + ' already exists '
     print '>>> Overwrite the existing database? '
     print '>>> y)es or n)o : ',
     yesNo = (raw_input()).lower()
     if(yesNo[0] != 'y'): 
          print 'Program terminated '
          sqDB.closeConnection()
          exit()
     else:
         sqCommand = 'DROP TABLE ' + runTableName + ';'
         sqDB.executeNoReturn(sqCommand)
  except sqLiteError :
    print 'Program terminated '
    sqDB.closeConnection()
    exit()
    
class TaskBuilder:
 def __init__(self,sqDB,runTableName,jobData,runParams,outputData):
     
    self.sqDB         = sqDB
    self.runTableName = runTableName
    #
    # Create keys and additional job data 
    #
    self.paramKeys = runParams.keys()
    paramKeyString = self.paramKeys[0]
    for i in range(1,len(self.paramKeys)):
        paramKeyString = paramKeyString + ':' + self.paramKeys[i] 
    
    self.outputKeys = outputData.keys()
    outputKeyString = self.outputKeys[0]
    for i in range(1,len(self.outputKeys)):
            outputKeyString = outputKeyString + ':' + self.outputKeys[i]
    #
    # Add jobData attributes that encode the run parameter attributes, 
    # output data attributes and job data attributes 
    #
    
    jobData['jobCreationDate']         = time.asctime()
    jobData['status']                  = 'task' 
    jobData['exec_id']                 = '' 
    jobData['runParameterAttributes']  = paramKeyString;
    jobData['outputDataAttributes']    = outputKeyString;
    jobData['jobDataNames']       = ' ';
            
    self.jobKeys = jobData.keys()
    jobKeyString = self.jobKeys[0]
    for i in range(1,len(self.jobKeys)):
            jobKeyString = jobKeyString + ':' + self.jobKeys[i] 

    jobData['jobDataNames']       = jobKeyString;
    
    #
    # Determine the SQlite types for the job data, run parameters, and output data
    #
    
    self.jobDataType = {}
    for i in range(len(self.jobKeys)):
        self.jobDataType[self.jobKeys[i]] = sqLite.getSQLiteType(jobData[self.jobKeys[i]]);
    
    self.paramType = {}
    for i in range(len(self.paramKeys)):
        self.paramType[self.paramKeys[i]] = sqLite.getSQLiteType(runParams[self.paramKeys[i]]);
    
    self.outputType = {}
    for i in range(len(self.outputKeys)):
        self.outputType[self.outputKeys[i]] = sqLite.getSQLiteType(outputData[self.outputKeys[i]]);
     
    #
    # Create the table 
    #
    tableString = ' CREATE TABLE ' + runTableName + ' ('

    tableString = tableString + self.jobKeys[0] + ' ' +  self.jobDataType[self.jobKeys[0]]

    for i in range(1,len(self.jobKeys)):
        tableString = tableString + ',' + self.jobKeys[i] + ' ' +   self.jobDataType[self.jobKeys[i]]
    
    for i in range(len(self.paramKeys)):
        tableString = tableString + ',' + self.paramKeys[i] + ' ' +  self.paramType[self.paramKeys[i]]

    for i in range(len(self.outputKeys)):
        tableString = tableString + ',' + self.outputKeys[i] + ' ' + self.outputType[self.outputKeys[i]]
    
    tableString = tableString + ');'
    
    try:
      sqMessage = self.sqDB.executeNoReturn(tableString) 
    except sqLiteError, e:                   
        print e.message
    
    
 def insertJobTask(self,jobData,runParams,outputData):  
    #
    # insert into table
    #
    insertString = 'INSERT INTO ' + self.runTableName + ' (' + self.jobKeys[0] 
    for i in range(1,len(self.jobKeys)):
        insertString = insertString +  ',' + self.jobKeys[i]
    
    for i in range(len(self.paramKeys)):
        insertString = insertString +  ',' + self.paramKeys[i]
        
    for i in range(len(self.outputKeys)):
        insertString = insertString +  ',' + self.outputKeys[i]
        
    insertString = insertString + ') VALUES (' 
    
    val     = jobData[self.jobKeys[0]]
    valType = self.jobDataType[self.jobKeys[0]]
    insertString = insertString + sqLite.insertVal(val,valType)
    
    for i in range(1,len(self.jobKeys)):
        val     = jobData[self.jobKeys[i]]
        valType = self.jobDataType[self.jobKeys[i]]
        insertString = insertString +  ',' + sqLite.insertVal(val,valType)

    for i in range(len(self.paramKeys)):
        val     = runParams[self.paramKeys[i]]
        valType = self.paramType[self.paramKeys[i]]
        insertString = insertString +  ',' + sqLite.insertVal(val,valType)
        
    for i in range(len(self.outputKeys)):
        val     = outputData[self.outputKeys[i]]
        valType = self.outputType[self.outputKeys[i]]
        insertString = insertString +  ',' + sqLite.insertVal(val,valType)
        
    insertString = insertString + ');'
    try:
      sqMessage = self.sqDB.executeNoReturn(insertString) 
    except sqLiteError, e:                   
        print e.message

    
 def showTasks(self,paramList,runDBname):
     showString = 'SELECT ROWID '
     
     for i in range(0,len(paramList)):
         showString = showString + ',' + paramList[i]
     
     showString = showString + ' FROM ' + self.runTableName + ';'
     
     try:
        if os.sys.platform == 'win32':
          sqMessage = self.sqDB.execute(showString).split("\r\n") 
        else:
          sqMessage = self.sqDB.execute(showString).split("\n") 
     except sqLiteError, e:                   
        print e.message
        
     runDBname  = os.path.basename(runDBname)
     
     print 
     print '########################################################'
     print '       Database --> %s <-- task list                    ' % runDBname
     print '########################################################'
     print 
     
     print 'Task_ID',
     for i in range(len(paramList)):
       print paramList[i],
     
     formatString = []
     strLength = len('Task ID')
     astring = "%" + '-%d'  % strLength
     astring = astring + 's'
     formatString.append(astring)
     
     for i in range(len(paramList)):
         strLength = len(paramList[i])
         astring = "%" + '-%d'  % strLength
         astring = astring + 's'
         formatString.append(astring)
         
     print '\n'
     for i in range(len(sqMessage)-1):
         sqLine = sqMessage[i].split('|')
         for j in range(len(sqLine)):
             print formatString[j]% sqLine[j],
         print ' '
 
     
 

 
     

    