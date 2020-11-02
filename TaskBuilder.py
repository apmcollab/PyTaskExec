import os
from string import Template
import time
import datetime
import sqlite3
import random

#############################################################################
#
# Copyright  2020 Chris Anderson
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the Lesser GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# For a copy of the GNU General Public License see
# <http://www.gnu.org/licenses/>.
#
#############################################################################

TRUE_VALS  = ( '1', 'true',  'True', 'TRUE')
FALSE_VALS = ( '0', 'false', 'False', 'FALSE')


from Classfetch import _get_func,_get_class
from io import StringIO


def getFullPath(fileName):
   if(os.path.isfile(fileName)):
       return os.path.abspath(fileName)
   else:
      print('   === Error ===')
      print(" Input file not found") 
      print(" Missing file = ", fileName)
      exit() 
  
def executeDBwithData(sqDB,sqCommand,data):
   tryCount = 0
   tryMax   = 100
   while(tryCount < tryMax):
     try :
       b = sqlite3.Binary(data)
       sqDB.execute(sqCommand,(b,))
       tryCount = tryMax
     except sqlite3.OperationalError as e:
       tryCount = tryCount +1
       print("Database locked. Retrying ...")
       sleepTime = .01*random.random()
       time.sleep(sleepTime)
           
def testRunDataFile(runTemplateFile,runData):
  #
  # 
  try :
    f = open(runTemplateFile,'rU')
  except IOError as exception:
      print('                 === Error ===')
      print(" Template file cannot be read") 
      print(exception)
      exit()
      
  runFileContents =  f.read()
  f.close()
  #
  # Verify that the template file is of Unix format 
  # (using construct from crlf.py in Toos/sripts)
  # 
  # Change as of 9/16/2020 - perhaps not needed anymore -
  # so commented out
  #if(runFileContents.find("\r\n")):
  #     newContents = runFileContents.replace("\r\n", "\n")
  #     f = open(runTemplateFile, "wb+")
  #     try:
  #      f.write(bytes(newContents,'UTF-8'))
  #     except TypeError:
  #       f.write(newContents)
  #     f.close()
  #
  # Check to make sure all of the parameters that are to be
  # substituted have template entries
  #
  runKeys = list(runData.keys())
  for i in range(len(runData)):
      findString = '$' + runKeys[i]
      findString = findString.strip()
      if(runFileContents.find(findString) == -1): 
          print('                 === Error ===')
          print(' Template variable \'' + findString +\
                '\' is not in the template file')
          print(' ' + runTemplateFile)
          exit()
  #
  # Substitute in the parameters
  #
  r  = Template(runFileContents)
  try:
   runDataFile = r.substitute(runData)
  except KeyError as exception:
   print('                 === Error ===')
   print('A parameter in: ' + runTemplateFile)
   print('has not been specified.\n')
   print('The parameter that needs to be specified :')
   print('runParams[',exception, ']')
   exit()
  
def checkForExistingDatabase(sqDB,sqCon,runDBname,runTableName,noCheck):
# Checks db for existing task table and removes if desired.
  
# The database file is automatically created when the 
# data base connection is open
  if(noCheck):
        sqCommand = 'DROP TABLE ' + runTableName + ';'
        try:
           sqDB.execute(sqCommand)
        except sqlite3.OperationalError:
           pass
        sqCommand = 'DROP TABLE ' + runTableName + '_support;'
        try:
           sqDB.execute(sqCommand)
        except sqlite3.OperationalError:
           pass
        return
      
  runDBname = os.path.basename(runDBname)
  sqCommand = 'SELECT ROWID from ' + runTableName + ' where ROWID = 1;'
  try:
     sqDB.execute(sqCommand)
     print('>>> The task database '  + runDBname + ' already exists ')
     print('>>> Overwrite the existing database? ')
     print('>>> y)es or n)o : ', end=' ')
     try:  
       yesNo = (raw_input()).lower()
     except NameError:
       yesNo  = (input()).lower()
     if(yesNo[0] != 'y'): 
          print('Program terminated ')
          sqDB.close()
          sqCon.close()
          exit()
     else:
         sqCommand = 'DROP TABLE ' + runTableName + ';'
         sqDB.execute(sqCommand)
         sqCommand = 'DROP TABLE ' + runTableName + '_support;'
         sqDB.execute(sqCommand)
  except sqlite3.OperationalError as e :
    pass

    
class TaskBuilder:
  def __init__(self,sqDB,sqCon,runTableName,taskData,jobData,runParams,outputData):
    self.sqDB         = sqDB
    self.runTableName = runTableName
    #
    # Create keys and additional job data 
    #
    self.paramKeys = list(runParams.keys())
    paramKeyString = self.paramKeys[0]
    for i in range(1,len(self.paramKeys)):
        paramKeyString = paramKeyString + ':' + self.paramKeys[i] 
    
    self.outputKeys = list(outputData.keys())
    outputKeyString = self.outputKeys[0]
    for i in range(1,len(self.outputKeys)):
            outputKeyString = outputKeyString + ':' + self.outputKeys[i]
            
    taskData['taskDate']                = time.asctime()
    taskData['taskTime']                = ''
    taskData['status']                  = 'task' 
    taskData['exec_id']                 = '' 
    taskData['task_id']                 = 0
            
    self.taskKeys = list(taskData.keys())
    taskKeyString = self.taskKeys[0]
    for i in range(1,len(self.taskKeys)):
            taskKeyString = taskKeyString + ':' + self.taskKeys[i] 

    #
    # Add jobData attributes that encode the run parameter attributes, 
    # output data attributes and job data attributes 
    #
  
    jobData['runParameterNames']  = paramKeyString;
    jobData['outputDataNames']    = outputKeyString;
    jobData['jobDataNames']       = ' ';
    jobData['taskDataNames']      = taskKeyString;
    
    self.jobKeys = list(jobData.keys())
    jobKeyString = self.jobKeys[0]
    for i in range(1,len(self.jobKeys)):
            jobKeyString = jobKeyString + ':' + self.jobKeys[i] 

    jobData['jobDataNames']       = jobKeyString;
    
    #
    # Determine the SQlite types for the task data, job data, run parameters, and output data
    #
    self.taskDataType = {}
    for i in range(len(self.taskKeys)):
        self.taskDataType[self.taskKeys[i]] = self.getSQLiteType(taskData[self.taskKeys[i]]);
    
    self.jobDataType = {}
    for i in range(len(self.jobKeys)):
        self.jobDataType[self.jobKeys[i]] = self.getSQLiteType(jobData[self.jobKeys[i]]);
    
    self.paramType = {}
    for i in range(len(self.paramKeys)):
        self.paramType[self.paramKeys[i]] = self.getSQLiteType(runParams[self.paramKeys[i]]);
    
    self.outputType = {}
    for i in range(len(self.outputKeys)):
        self.outputType[self.outputKeys[i]] = self.getSQLiteType(outputData[self.outputKeys[i]]);
     
    #
    # Create the table 
    #
    tableString = ' CREATE TABLE ' + runTableName + ' ('

    tableString = tableString + self.taskKeys[0] + ' ' +  self.taskDataType[self.taskKeys[0]]

    for i in range(1,len(self.taskKeys)):
        tableString = tableString + ',' + self.taskKeys[i] + ' ' +   self.taskDataType[self.taskKeys[i]]
    
    for i in range(len(self.paramKeys)):
        tableString = tableString + ',' + self.paramKeys[i] + ' ' +  self.paramType[self.paramKeys[i]]

    for i in range(len(self.outputKeys)):
        tableString = tableString + ',' + self.outputKeys[i] + ' ' + self.outputType[self.outputKeys[i]]
    
    tableString = tableString + ');'
    
    try:
      sqDB.execute(tableString) 
    except sqlite3.OperationalError as e:                   
        print(e.message)
        
    #
    # Create the job data table 
    #
    fileNames  = jobData["fileNames"].split(":")
    fileDataNames = []
    for i in fileNames:
      fileDataNames.append(i + '_data')
  
    tableString = ' CREATE TABLE ' + runTableName + '_support ('

    tableString = tableString + self.jobKeys[0] + ' ' +  self.jobDataType[self.jobKeys[0]]

    for i in range(1,len(self.jobKeys)):
        if(self.jobKeys[i] in fileDataNames) : 
          tableString = tableString + ',' + self.jobKeys[i] + ' BLOB ' 
        else:
          tableString = tableString + ',' + self.jobKeys[i] + ' ' +   self.jobDataType[self.jobKeys[i]]
   
    tableString = tableString + ');'
    
    try:
      sqDB.execute(tableString) 
    except sqlite3.OperationalError as e:                   
        print(e.message)
    #
    # insert non-file job information into the table
    #

    
    insertString = 'INSERT INTO ' + self.runTableName + '_support (' 
    
    insertFlag = False 
    for i in range(0,len(self.jobKeys)):
      val     = jobData[self.jobKeys[i]]
      valType = self.jobDataType[self.jobKeys[i]]
      if(valType != "BLOB"):
        if(insertFlag == True ) :
          insertString = insertString +  ',' + self.jobKeys[i]
        else: 
          insertString = insertString + self.jobKeys[i]
          insertFlag = True
          
    insertString = insertString + ') VALUES (' 
    
    insertFlag = False 
    for i in range(0,len(self.jobKeys)):
        val     = jobData[self.jobKeys[i]]
        valType = self.jobDataType[self.jobKeys[i]]
        if(valType != "BLOB"):
          if(insertFlag == True ) : 
            insertString = insertString +  ',' + self.insertVal(val,valType)
          else : 
            insertString = insertString + self.insertVal(val,valType)
            insertFlag   = True
  
    insertString = insertString + ');'
    try:
      sqDB.execute(insertString) 
    except sqlite3.OperationalError as e:                   
      print(e)
      
    #
    # insert job file information into the table
    #
    for i in fileNames:
      fName = i + '_name'
      if(fName != 'outputHandlerClass_name'):
        testTextFile = getFullPath(jobData[fName])
        fileobj = open(testTextFile, mode='rb')
        inputFile = fileobj.read()
        fileobj.close()
        sqCommand  = "UPDATE " + self.runTableName + '_support SET ' + i + '_data = (?) where rowid = 1;'
        executeDBwithData(sqDB,sqCommand,inputFile)
#
#   Import the class to validate the output handler class's syntax.
#
    try:
      outputHandlerName = jobData['outputHandlerClass_name']
    except KeyError as e:
      try: 
        outputHandlerScript = jobData['outputHandlerScript']
      except KeyError as e:
        return 
      #
      # Checking output handler specified by a file name
      #
      os.sys.path.append(os.path.dirname(outputHandlerScript))
      outputHandlerName  = os.path.basename(outputHandlerScript).split('.')[0]
      outputHandlerName  = outputHandlerName + '.' + outputHandlerName
      try:
        outputHandler          = _get_class(outputHandlerName)
        return
      except ImportError as exception:
        print('Failed to load output handler specified by ' + outputHandlerScript)
        print(exception)
        exit()
      except AttributeError as exception:
        print('Failed to load output handler specified by ' + outputHandlerScript)
        print(exception)
        exit()
    #
    # Checking the class inserted into the database
    #
    os.sys.path.append(os.path.dirname(jobData['outputHandlerScript_name']))
    testClass = _get_class(outputHandlerName + "." + outputHandlerName)
    return
      
        
  def insertVal(self,val,valType):
     if(valType == 'TEXT') : return  "\'" + val.strip() + "\'"
     if(valType == 'BOOL') : 
       if((type(val) is bool) and (val == True)) : return "\'true\'"
       if((type(val) is bool) and (val == False)): return "\'false\'"
       if((type(val) is int) and (val == 1)) : return "\'true\'"
       if((type(val) is int) and (val == 0)): return "\'false\'"
       if(val.strip() in TRUE_VALS): 
            return "\'true\'"
       if(val.strip() in FALSE_VALS): 
            return "\'false\'"
     if((valType == 'INTEGER') and (not (type(val) is int))):
      print('                 === Error ===')
      print('Parameter type ambiguity \n')
      print('Use explicit type specification.\n')
      print('Auto detected type : ', end = ' ') 
      print(valType)
      print('Actual type        : ',end = ' ')
      print(type(val))
      exit()
      
     if(valType == 'INTEGER'): return '%d'      % val
     if(valType == 'REAL'):    return '%-.16e'  % val
     if(valType == 'BLOB'):    return val
     
  def getSQLiteType(self,val):
     if(type(val) is int  ): return 'INTEGER'
     if(type(val) is bool ): return 'BOOL'
     if(type(val) is str  ): return 'TEXT' 
     try:
       if(type(val) is unicode ): return 'TEXT' 
     except NameError:
       if(type(val) is bytes  ): return 'TEXT' 
     if(type(val) is float    ): return 'REAL'
     return 'BLOB'
   
  def getSQLiteValue(self,val):
     if(type(val) is int    ): return '%d'      % val
     if((type(val) is bool) and (val == True)) : return "\'true\'"
     if((type(val) is bool) and (val == False)): return "\'false\'"
     if(type(val) is str    ): return  "\'" + val.strip() + "\'" 
     try: 
       if(type(val) is unicode   ): return  "\'" + val.strip() + "\'" 
     except NameError:
       if(type(val) is bytes     ): return  "\'" + val.strip() + "\'" 
     if(type(val) is float  ): return '%-.16e'  % val
     return val
   

  def insertJobTask(self,sqDB,sqCon,taskData,runParams,outputData): 
    #
    # insert into table
    #
    insertString = 'INSERT INTO ' + self.runTableName + ' (' + self.taskKeys[0] 
    for i in range(1,len(self.taskKeys)):
        insertString = insertString +  ',' + self.taskKeys[i]
        
    for i in range(len(self.paramKeys)):
        insertString = insertString +  ',' + self.paramKeys[i]
        
    for i in range(len(self.outputKeys)):
        valType = self.outputType[self.outputKeys[i]]
        if(valType != "BLOB"):
          insertString = insertString +  ',' + self.outputKeys[i]
 
    insertString = insertString + ') VALUES (' 
    
    val     = taskData[self.taskKeys[0]]
    valType = self.taskDataType[self.taskKeys[0]]
    insertString = insertString + self.insertVal(val,valType)
    
    for i in range(1,len(self.taskKeys)):
        val     = taskData[self.taskKeys[i]]
        valType = self.taskDataType[self.taskKeys[i]]
        insertString = insertString +  ',' + self.insertVal(val,valType)
  
    for i in range(len(self.paramKeys)):
        val     = runParams[self.paramKeys[i]]
        valType = self.paramType[self.paramKeys[i]]
        insertString = insertString +  ',' + self.insertVal(val,valType)
        
    for i in range(len(self.outputKeys)):
        val     = outputData[self.outputKeys[i]]
        valType = self.outputType[self.outputKeys[i]]
        if(valType != "BLOB"):
          insertString = insertString +  ',' + self.insertVal(val,valType)
   
    insertString = insertString + ');'
    try:
      sqDB.execute(insertString) 
    except sqlite3.OperationalError as e:                   
      print(e)

    
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
     except sqlite3.Error as e:                   
        print(e)
        
     runDBname  = os.path.basename(runDBname)
     
     print() 
     print('########################################################')
     print('       Database --> %s <-- task list                    ' % runDBname)
     print('########################################################')
     print() 
     
     print('Task_ID', end=' ')
     for i in range(len(paramList)):
       print(paramList[i], end=' ')
     
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
         
     print('\n')
     for i in range(len(sqMessage)-1):
         sqLine = sqMessage[i].split('|')
         for j in range(len(sqLine)):
             print(formatString[j]% sqLine[j], end=' ')
         print(' ')


 
     



    
