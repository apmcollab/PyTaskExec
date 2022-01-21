import optparse
import os
from socket import gethostname
from shutil import copy
from SqUtilities import SqUtilities
#   
#
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
import sqlite3
from string import Template
from Classfetch import _get_func, _get_class
from hyperlink._url import NoneType

class CreateTaskDataFiles(object):
  def __init__(self):
    
    self.standardOptions = {}
    self.standardOptions['runDBname']     = ''
    self.standardOptions['includePaths']  = ''
    self.standardOptions['outputDir']     = 'TaskData'
    self.standardOptions['nExec']         = 1
    self.standardOptions['batch_submit']  = ''
    self.standardOptions['silent']        = False
    self.standardOptions['pythonProgram'] = ''
    self.standardOptions['exec_id']       = None
    self.standardOptions['localCache']    = True
    
    self.standardOptions['prefix_Task_ID'] = None
    self.standardOptions['multipleInstance']      = False
    self.standardOptions['fileBasedCoordination'] = True
    
    self.sqUtilities  = SqUtilities()
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

  def parseStandardOptions(self):
     p = optparse.OptionParser(conflict_handler="resolve")
     
     p.add_option('--database','-d',action='store',dest='run_database',\
                 help='Specifies job task database file (required)')
     
     p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
     
     p.add_option('--include','-I',action='store',dest='include_paths',\
                 help="""Additional search paths for required files.   
                         Multiple entries separated by ;""")
     
     p.add_option('--output','-o',action='store',dest='output_directory',\
                 help='Parent directory of TaskExec task directories')
     
     p.add_option('--nExec','-n',action='store',type='int',default=0,\
             dest='exec_count', help='Number of TaskExec instances to start')
     
     p.add_option('--batch','-b',action='store',\
             dest='batch_submit', help='Run in batch mode using specified python batch submission program')
 
     p.add_option('--silent','-s',action='store_true',\
             dest='silent', help='Suppress output of programs run by TaskExec')
     
     p.add_option('--local','-l',action='store_true',\
             dest='local_cache', help='Create a local cache of the task data base file in the task directories to minimize db access conflicts. Default True.')
     
     p.add_option('--no-local',action='store_false',\
             dest='local_cache', help='Don\'t use a local cache of the task data base. Default False.')
   
     p.add_option('--python','-p',action='store',\
             dest='python_program', help='Python program (full path)')
     
     p.add_option('--execID','-e',action='store',type='string',\
             dest='exec_id', help='Specify name portion of TaskExec ID identification')
     
     p.add_option('--alternateTaskPrefix','-a',action='store',type='string',\
             dest='prefix_Task_ID', help='Specifies an alternate prefix to the task output file name. Default name is Task, resulting in output file names of the form Task_XXX.') 

     p.add_option('--multiple','-m',action='store_true',\
             dest='multiple_instance', help='An additional instance of ExecRun being started. Supresses portions of the startup procedure.')

     p.add_option('--filecoord','-f',action='store_true',\
             dest='file_coordination', help='Use file based task coordination, rather than db coordination. Default true.')

     p.add_option('--no-filecoord',action='store_false',\
             dest='file_coordination', help='Use db coordination. Default false.')

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
       
     if(options.include_paths != None):
         self.standardOptions['includePaths'] = options.include_paths
         includePaths = options.include_paths.split(';')
         for i in range(len(includePaths)):
             os.sys.path.append(includePaths[i])
       
     if(options.output_directory):
         self.standardOptions['outputDir']  = options.output_directory
        
     if(options.exec_count):
         self.standardOptions['nExec']  = options.exec_count
             
     if(options.batch_submit):
         self.standardOptions['batch_submit'] = options.batch_submit

     if(options.local_cache != None):
         self.standardOptions['localCache'] = options.local_cache
         
     if(options.silent != None):
         self.standardOptions['silent'] = options.silent   
         
     if(options.python_program != None):
         self.standardOptions['pythonProgram'] = options.python_program 
    
     if(options.exec_id):
         self.standardOptions['exec_id'] = options.exec_id
     else:
       hostNameStart = gethostname().lower()
       hostName      = hostNameStart.split(".")[0]
       self.standardOptions['exec_id']  = hostName
       
     if(options.prefix_Task_ID):
       self.standardOptions['prefix_Task_ID'] = options.prefix_Task_ID
       
     if(options.multiple_instance):
         self.standardOptions['multipleInstance'] = options.multiple_instance 
         
     if(options.file_coordination != None):
       self.standardOptions['fileBasedCoordination'] = options.file_coordination

  
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
       
     if('include_paths' in options):
         self.standardOptions['includePaths'] = options['include_paths']
         includePaths = options.include_paths.split(';')
         for i in range(len(includePaths)):
             os.sys.path.append(includePaths[i])
       
     if('output_directory' in options):
         self.standardOptions['outputDir']  = options['output_directory']
        
     if('exec_count' in options):
         self.standardOptions['nExec']  = options['exec_count']
             
     if('batch_submit' in options):
         self.standardOptions['batch_submit'] = options['batch_submit']

     if('silent' in options):
         self.standardOptions['silent'] = options['silent']   
            
     if('local_cache' in options):
         self.standardOptions['localCache'] = options['local_cache']
     else:
         self.standardOptions['localCache'] = True
    
     if('python_program' in options):
         self.standardOptions['pythonProgram'] = options['python_program']
         
     if('multiple_instance' in options):
         self.standardOptions['multipleInstance'] = options['multiple_instance']  
         
     if('file_coordination' in options):
       self.standardOptions['fileBasedCoordination'] = options['file_coordination']
     else:
       self.standardOptions['fileBasedCoordination'] = True
       
     if('prefix_Task_ID' in options):
       self.standardOptions['prefix_Task_ID'] = options['prefix_Task_ID']
     else:
       self.standardOptions['prefix_Task_ID']= None
        
     if('exec_id' in options):
         self.standardOptions['exec_id'] = options['exec_id']
     else:
       hostNameStart = gethostname().lower()
       hostName      = hostNameStart.split(".")[0]
       self.standardOptions['exec_id']  = hostName
#
#===========     Begin Main Program   ============================
#
  def run(self):
    #
    # Open up the database and extract the total task count and a list of
    # tasks to be done. 
    #
    self.jobDBname       = self.standardOptions['runDBname']
    self.outputDirectory = self.standardOptions['outputDir'] 
    
    print(self.getFileFullPath(self.jobDBname))
    
    
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

    taskCount             = 0
  
  # capture total task count if required
  
    if(self.totalTaskCount == None):
      sqCon  = sqlite3.connect(self.standardOptions['runDBname'],isolation_level=None)
      sqDB   = sqCon.cursor()

      sqCommand = "SELECT count(status) FROM " + self.runTableName + "; "
      self.sqUtilities.executeDB(sqDB,sqCommand)
      self.totalTaskCount = sqDB.fetchall()[0][0]
      sqDB.close()
      sqCon.close()  
    
    for i in range(0,self.totalTaskCount):
      self.taskID = '%d'% (i+1)
      self.createRunWorkingDirectory()
      self.captureTaskData()
      self.createRunDataFile()
    
    print("XXXXXXXXX ExecRun Finished XXXXXXXXXXXXXX") 

#
#===========     End Main Program   ============================
#  


  def captureTaskData(self):
    #
    # Caputure the task attributes 
    #
    self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
    localDBname = None
    #
    # On initial pass capture the input template file and output handler 
    #
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
      
    sqCommand              = 'SELECT outputDataNames from ' + self.runTableName + '_support WHERE ROWID = 1;'
    self.sqUtilities.executeDB(sqDB,sqCommand)
    outputDataNames   = sqDB.fetchall()[0]
    self.outputKeys    = outputDataNames[0].split(':');
    
    for i in range(len(self.outputKeys)):
      sqCommand  = 'SELECT typeof(' + self.outputKeys[i] + ') from ' + self.runTableName + ' WHERE ROWID = 1;'
      self.sqUtilities.executeDB(sqDB,sqCommand)
      self.outputDataType[self.outputKeys[i]] = sqDB.fetchone()[0]
    
    sqCommand              = 'SELECT taskDataNames from ' + self.runTableName + '_support WHERE ROWID = 1;'
    self.sqUtilities.executeDB(sqDB,sqCommand)
    taskDataNames   = sqDB.fetchall()[0]
    self.taskKeys   = taskDataNames[0].split(':');
    
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
    
    self.importTemplateFileAndOutputHandler()
  #
  # Standard call 
  #
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
    
    for i in range(len(self.taskKeys)):
      sqCommand  = 'SELECT ' + self.taskKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
      self.sqUtilities.executeDB(sqDB,sqCommand)
      val = sqDB.fetchone()
      if(type(val) != NoneType) :
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
    
    print('Creating data in : ' + fName )
    self.writeToUnixFile(bytes(self.runDataFile, encoding='utf8'), fName)
  
  
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
    
  def importTemplateFileAndOutputHandler(self):
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
