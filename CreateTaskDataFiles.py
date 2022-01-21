import optparse
import os
import string
from io import StringIO
from string import Template
from socket import gethostname
from shutil import copy
from SqUtilities import SqUtilities
#
# This program creates the set of data files associated all the
# tasks specified in the data base created using TaskDbBuilder. 
#
# Files required to run ExecRun are NOT created, and so the 
# presumpation is another method of executing the program 
# in each of the data file directories is being used. 
#
# The invocation of this program is identical to that for 
# ExecRun, i.e. 
#
# python3 [Path to PyTaskExec]/CreateTaskDataFiles.py -d [TaskDataBase] -t [TaskTable] -o [Output directory]
#
# If the output directory parameter is not specified, then the directory containing
# all the task directories is named "TaskData"
#
# The input task database is not altered. 
#   
#
#############################################################################
#
# Copyright  2022 Chris Anderson
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
import sqlite3
from string import Template


class CreateTaskDataFiles(object):
  def __init__(self):
    
    self.standardOptions = {}
    self.standardOptions['runDBname']       = ''
    self.standardOptions['outputDir']       = 'TaskData'
    self.standardOptions['runTableName']    = ''

    
    self.sqUtilities    = SqUtilities()
    self.tasksToBeDone  = 0
    self.totalTaskCount = None
    
    self.taskID         = 0;
    self.runTableName   = None
    self.firstCall      = True
    self.jobDBname      = None
    self.runTemplate    = None
    self.alternateOutputDirectory = '.'
    
    self.outputKeys = {}
    self.paramKeys  = {}
    self.jobKeys    = {}
    self.jobData    = {}
    self.taskData   = {}
    self.taskKeys   = {}
    self.runData     = {}
    self.runDataType = {}
    self.outputData = {}
    self.outputDataType = {}

  def parseStandardOptions(self):
     p = optparse.OptionParser(conflict_handler="resolve")
     
     p.add_option('--database','-d',action='store',dest='run_database',\
                 help='Specifies job task database file (required)')
     
     p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
  
     p.add_option('--output','-o',action='store',dest='output_directory',\
                 help='Parent directory of TaskExec task directories')

     options,arguments = p.parse_args()
    
     if(not(options.run_database)): 
         print('Run database file name must be specified')
         print('Specify using --database filename or -d filename ')
         exit()
     else:
         self.standardOptions['runDBname'] = options.run_database

     if(options.task_table): 
       self.standardOptions['runTableName']  = options.task_table
     else:   
       self.standardOptions['runTableName'] = (os.path.basename(self.standardOptions['runDBname']).split('.'))[0] 
       
     if(options.output_directory):
         self.standardOptions['outputDir']  = options.output_directory

  
  def setStandardOptions(self,options):
     if(not('run_database' in options)): 
         print('Run database file name must be specified')
         print('Specify using --database filename or -d filename ')
         exit()
     else:
         self.standardOptions['runDBname'] = options['run_database']

     if('task_table' in options): 
       if(self.standardOptions['runTableName'] != None):
         self.standardOptions['runTableName']  = options['task_table']
       else:
         self.standardOptions['runTableName']   = (os.path.basename(self.standardOptions['runDBname']).split("."))[0]
     else:        
       self.standardOptions['runTableName']     = (os.path.basename(self.standardOptions['runDBname']).split("."))[0]
     
     if('output_directory' in options):
         self.standardOptions['outputDir']  = options['output_directory']
        
#
#===========     Begin Main Program   ============================
#
  def run(self):
    #
    # Open up the database and extract the total task count and a list of
    # tasks to be done. 
    #
    
    print("XXXXXXXXX  CreateTaskDataFiles XXXXXXXXXXXXXXXXX")
    print()
    self.jobDBname       = self.standardOptions['runDBname']
    self.outputDirectory = self.standardOptions['outputDir'] 
    
    print("Task database              : " + self.getFileFullPath(self.jobDBname))
    print("Task data files directory  : " + self.outputDirectory)
    print()
    
    self.runTableName = self.standardOptions['runTableName'] 
    try :
      sqCon        = sqlite3.connect(self.standardOptions['runDBname'],isolation_level=None)
      sqDB         = sqCon.cursor()
      sqCommand    = 'SELECT status FROM ' + self.runTableName + ';' 
      sqDB.execute(sqCommand)
      taskListData = sqDB.fetchall()
      sqDB.close()
      sqCon.close()
    except sqlite3.OperationalError as e:
      print(e)
      exit()
      
    taskCount     = taskListData.__len__()
    taskList      = []
    self.tasksToBeDone = 0
    for i in range(0,taskCount):
      ip1 = i+1
      if(taskListData[i][0] == 'task'):
        self.tasksToBeDone = self.tasksToBeDone + 1
        taskList.append(ip1)
    
    for i in range(0,taskCount):
      self.taskID = '%d'% (i+1)
      self.createRunWorkingDirectory()
      self.captureTaskData()
      self.createRunDataFile()
    
    print()
    print("XXXXXXXXX CreateTaskDataFiles  Finished XXXXXXXXXXXXXX") 
#
#===========     End Main Program   ============================
#  
  def captureTaskData(self):
    self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'

    if(self.firstCall):
      self.firstCall = False
  
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
  
    sqCommand = 'SELECT jobDataNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
    self.sqUtilities.executeDB(sqDB,sqCommand)
    jobDataNames =  sqDB.fetchall()[0]
    self.jobKeys = jobDataNames[0].split(':')
    
    for i in range(len(self.jobKeys)):
      sqCommand  = 'SELECT ' + self.jobKeys[i] + ' from ' + self.runTableName + '_support  WHERE ROWID = 1;'
      self.sqUtilities.executeDB(sqDB,sqCommand)
      self.jobData[self.jobKeys[i]]=sqDB.fetchone()[0]
      
    sqCommand              = 'SELECT runParameterNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
    self.sqUtilities.executeDB(sqDB,sqCommand)
    runParameterNames = sqDB.fetchall()[0]
    self.paramKeys         = runParameterNames[0].split(':');
    
    for i in range(len(self.paramKeys)):
      sqCommand  = 'SELECT typeof(' + self.paramKeys[i] + ') from ' + self.runTableName + ' WHERE ROWID = 1;'
      self.sqUtilities.executeDB(sqDB,sqCommand)
      self.runDataType[self.paramKeys[i]] = sqDB.fetchone()[0]

    sqDB.close()
    sqCon.close()
    
    self.importTemplateFile()
  #
  # Using template, create input file 
  #
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
    
    for i in range(len(self.taskKeys)):
      sqCommand  = 'SELECT ' + self.taskKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
      self.sqUtilities.executeDB(sqDB,sqCommand)
      val = sqDB.fetchone()
      if(type(val) != type(None)) :
        self.taskData[self.taskKeys[i]] = val[0]
           
    for i in range(len(self.outputKeys)):
      sqCommand  = 'SELECT ' + self.outputKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
      self.sqUtilities.executeDB(sqDB,sqCommand)
      self.outputData[self.outputKeys[i]] = sqDB.fetchone()[0]
  
    for i in range(len(self.paramKeys)):
      sqCommand  = 'SELECT ' + self.paramKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
      self.sqUtilities.executeDB(sqDB,sqCommand)
      self.runData[self.paramKeys[i]] = sqDB.fetchone()[0]

    sqDB.close()
    sqCon.close()

  def createRunDataFile(self):
    #
    # Substitute in the parameters
    #
    substituteData = {}
    for i in range(len(self.paramKeys)):
      substituteData[self.paramKeys[i]] = (self.substituteVal(self.runData[self.paramKeys[i]],self.runDataType[self.paramKeys[i]]))
    
    self.runDataFile =  self.runTemplate.substitute(substituteData)
    self.runFileName  = self.jobData['runFileName']
    fName       =  self.workDirName + os.path.sep + self.runFileName
    
    print('Creating data   : ' + fName )
    self.writeToUnixFile(bytes(self.runDataFile, encoding='utf8'), fName)
    
 
    fileNames = self.jobData['fileNames'].split(":")
    for i in fileNames:
      if((i != 'outputHandlerClass') \
         and (i != 'outputHandlerScript') \
         and (i != 'runFileTemplate') \
         and (i != 'xmlTaskFile')):
        fName = self.workDirName + os.path.sep + os.path.basename(self.jobData[i + '_name'])
        fileHandle   = StringIO(self.jobData[i + '_data'].decode('utf-8'))
        dataFileTmp  = fileHandle.read() 
        fileHandle.close()
        print('Creating files  : ' + fName )
        self.writeToUnixFile(dataFileTmp , fName)   
    print() 
  
  
  def createRunWorkingDirectory(self):
    #
    # Write out the data file to the run directory 
    #
    self.taskIDout = self.taskID
    if   (int(self.taskID) <= 9)  : self.taskIDout = '00' + self.taskID
    elif (int(self.taskID) <= 99) : self.taskIDout = '0'  + self.taskID
    else                          : self.taskIDout = self.taskID
  
    if (not (os.path.isdir(self.outputDirectory))) : os.mkdir(self.outputDirectory)
        
    self.workDirName =  self.outputDirectory + os.path.sep + self.runTableName + '_' + self.taskIDout
    if (not (os.path.isdir(self.workDirName))) : os.mkdir(self.workDirName)
  
  def substituteVal(self,val,valType):
     if(valType == 'text'):    return val
     if(valType == 'integer'): return '%d'      % val
     if(valType == 'real'):    return '%-.16e'  % val
     if(valType == 'blob'):    return val
     
  def writeToUnixFile(self,fileContents,fileName):
    f = open(fileName, "wb+")
    if(type(fileContents) is bytes ):
      f.write(fileContents)
    else:
      f.write(fileContents.encode())
    f.close()    
 
  def getFileFullPath(self,fileName):
    if(os.path.isfile(fileName)): 
      return os.path.abspath(fileName)
  
    for i in range(len(os.sys.path)):
      fullPathName = os.sys.path[i] + os.path.sep + fileName
      if(os.path.isfile(fullPathName)): 
        return os.path.abspath(fullPathName)
    
    print('                 === Error ===')
    print(fileName + " not found") 
    exit() 
      
  def getDirFullPath(self,dirName):
    if(os.path.isdir(dirName)): 
      return os.path.abspath(dirName)
  
    for i in range(len(os.sys.path)):
      fullPathName = os.sys.path[i] + os.path.sep + dirName
      if(os.path.isdir(fullPathName)): 
        return os.path.abspath(fullPathName)
    raise LookupError 
    
  def importTemplateFile(self):
    #
    # Extract the the template file from the database and create a template from it
    #
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
  
    templateDataName     = 'runFileTemplate_data'
    sqCommand = "SELECT " + templateDataName + '  FROM '  + self.runTableName + "_support where rowid = 1; "
    self.sqUtilities.executeDB(sqDB,sqCommand)
    dataFile     = sqDB.fetchall()[0][0]
    sqDB.close()
    sqCon.close()
    self.runTemplate = Template(dataFile.decode('utf8'))
#
#   Stub for executing the class defined in this file 
#   
if __name__ == '__main__':
  createTaskDataFile = CreateTaskDataFiles()
  createTaskDataFile.parseStandardOptions()
  createTaskDataFile.run()
