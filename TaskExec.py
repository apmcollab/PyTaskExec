import os
import shutil
import string
import subprocess
import time
import string
import random
from   string import Template
import optparse
from socket import gethostname
import pickle
from io import StringIO
import sys
from types import *

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


from Classfetch import _get_func, _get_class

import sqlite3

class TaskExec(object):
 """
 Executes tasks specified by an job task database.
 """ 
 def __init__(self):
    random.seed()
    self.fileBasedCoordination = False
    self.taskConflictSleepFactor       = 0.01
    self.readDBconflictStartingFactor  = 0.01
    self.readDBconflictBoundingFactor  = 2.0
    self.minimalTaskTime               = 2.0
    self.silentRun        = False
    self.multipleInstance = False
    hostNameStart = gethostname().lower()
    hostName = hostNameStart.split(".")[0]
    self.exec_id          = hostName + "_0" # default ID used for single instance runs.
    self.importRequiredModules()
    self.localDirectory    = os.getcwd()
    self.doneIDfileName    = None 
    self.outputHandlerFlag = False
    self.outputHandler     = None
    self.firstCall         = True
    self.localCache        = False
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
    self.taskExclusions = []
    self.taskAvailable    = True
    self.taskID           = '0'
    self.execRunfileName   = None
    self.execDonefileName  = None
    self.totalTaskCount    = None
    self.prefix_Task_ID    = None
    self.ssh_run           = None
    self.sequential        = False

#
#===========     Begin Main Program   ============================
#  
 def run(self):
  taskCount             = 0
  
  # capture total task count if required
  
  if(self.totalTaskCount == None):
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level=None)
    sqDB   = sqCon.cursor()

    sqCommand = "SELECT count(status) FROM " + self.runTableName + "; "
    self.executeDB(sqDB,sqCommand)
    self.totalTaskCount = sqDB.fetchall()[0][0]

    sqDB.close()
    sqCon.close()  
  
  if (not (os.path.isdir(self.alternateOutputDirectory))): os.mkdir(self.alternateOutputDirectory)
  self.execRunfileName   = os.path.abspath(self.alternateOutputDirectory) + os.path.sep +'.execTask'
  self.execDonefileName  = os.path.abspath(self.alternateOutputDirectory) + os.path.sep +'.doneTask'
  if(not os.path.isfile(self.execRunfileName)):
    f = open(self.execRunfileName, "a")
    f.close()    
  if(not os.path.isfile(self.execDonefileName)):
    f = open(self.execDonefileName, "a")
    f.close()    

      
  while((self.taskAvailable)and((taskCount < self.maxTaskCount) or (self.maxTaskCount == 0))):
      self.getTaskID();
      if(not self.taskAvailable): 
        break
      
      # Create a working directory for the run
      self.createRunWorkingDirectory()
      
      # write exec_id to .taskID file to place a claim on the task
      if(self.multipleInstance):
        self.writeExecIDtoTaskIDfile()
      #
      # check to make sure that this TaskExec is the first to work on the task
      # Also check to make sure file exists;it may not if multiple ExecRun's are
      # started. 
      #
      if(self.multipleInstance):
       if(os.path.isfile(self.taskIDfileName)):
         taskIDfile  = open(self.taskIDfileName, "r")
         firstCaptureID = taskIDfile.readline()
         taskIDfile.close()
         if(firstCaptureID.strip()  != self.exec_id.strip()):
           self.taskExclusions.append(int(self.taskID))
           continue   
       else:
          sleepTime = self.taskConflictSleepFactor*random.random()
          time.sleep(sleepTime)
          continue   
       
      print(self.exec_id + ' working on task ' + self.taskID)
      self.taskExclusions.append(int(self.taskID))
      #
      # Write to the database that we're taking care of the task originally isolation level = None
      #
      if(self.fileBasedCoordination):
        self.writeToExecRunFile()
        #
        # In file based coordination, the should be no conflicts, so try to include
        # an update here so the db can be viewed for update status. 
        #
        sqCon  = sqlite3.connect(self.jobDBname,isolation_level=None)
        sqDB   = sqCon.cursor()
        self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
        sqCommand    = 'UPDATE ' + self.runTableName \
                      + " SET status = \'exec\', exec_id = " \
                      + "\'" + self.exec_id  + "\'" +  self.sqIDtailString 
                      
        self.executeDB(sqDB,sqCommand)
        sqDB.close()
        sqCon.close()
      else:
        sqCon  = sqlite3.connect(self.jobDBname,isolation_level=None)
        sqDB   = sqCon.cursor()
        self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
        sqCommand    = 'UPDATE ' + self.runTableName \
                      + " SET status = \'exec\', exec_id = " \
                      + "\'" + self.exec_id  + "\'" +  self.sqIDtailString 
                      
        self.executeDB(sqDB,sqCommand)
        sqDB.close()
        sqCon.close()  
      #
      # Caputure the task attributes and data
      #
      self.captureTaskData()
      #
      # Create the run data file using the input template and run data 
      #
      self.createRunDataFile()   
      #
      # Execute the command in the working directory
      #
      if(not self.silentRun): 
        print('Running code')
      os.chdir(self.workDirName)
      runCommand = self.jobData['executableCommand']
      
      #
      # If ssh batch, then read in ssh submit script to
      # obtain the ssh command prefix that specifies 
      # which machine to run this exec on. 
      #
      #
      #if(self.ssh_run != None):
      #  sshSubmissionClass = self.getFileFullPath(self.ssh_run)
      #  sshClass           = self.importSSHsubmitClass(sshSubmissionClass)
      #  sshSubmitProgram    = sshClass(os.path.dirname(sshSubmissionClass))
      #sshSubmitProgram.getSSHremoteCommand()
      # runCommand = sshSubmitProgram.getSSHremoteCommand() + " " + runCommand
      
      if(not self.silentRun): 
        print(runCommand)
    
      startTime = time.time()
      p = subprocess.Popen(runCommand,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      (r,e) = (p.stdout, p.stderr)
      self.runOutput = r.read().decode('utf8');
      sqError        = e.read().decode('utf8');
      if(os.path.sep == '\\') : 
        sqError = sqError.replace("\r\n",'')
      else:                     
        sqError = sqError.replace("\n",'')
      if(sqError != ''): 
        print(sqError)
      r.close()
      e.close()        
      self.taskTime = time.time() - startTime
      
      if(self.prefix_Task_ID != None):
        ExecOutputName = self.prefix_Task_ID + '_' + self.taskIDout  + '.output'
      else:
        ExecOutputName = 'Task_'+ self.taskIDout  + '.output'
      fileOutput     = self.runOutput.replace("\r\n","\n")
      outf = open( ExecOutputName, "wb+")
      outf.write(bytes(fileOutput,encoding='utf8'))
      outf.close()
       
      if(not self.silentRun): 
        print('Done')

      if(not self.silentRun): 
         printOutput = self.runOutput.replace("\r\n","\n")
         print(printOutput)

      #  process the output and capture output data 
    
      self.handleProgramOutput()
      
      #
      # Change back to local directory 
      #
      os.chdir(self.localDirectory)

      #  pack the output into the database
    
      self.packOutput()
      
      self.writeToExecDoneFile()
    
      taskCount = taskCount + 1
    
 #############################################################
 #    End of while loop in run()  
 #############################################################
 #
 # Call the finalizer routine
  self.finalize()
 #############################################################
 #    End of run()  
 #############################################################
 #  
#
# This routine reads from the command line (if specified) and
# initializes (with defaults if not specified) the class variables
#
# jobDBname : 
#     The job database specifying tasks associated with the job
#
# alternateOutputDirectory :
#     An alternate directory for the program output associated with each task
#
# maxTaskCount:
#     The maximal number of tasks to be performed. 0 (default) indicates
#     an unlimited number (e.g. until all the tasks in the job database
#     have been completed).
#
# The routine also adds to the system path any paths specified using the
# -I or --include option.
#
 
 def parseStandardOptions(self):
     p = optparse.OptionParser()
     p.add_option('--database','-d',action='store',dest='run_database',\
                 help='Specifies job task database file (required)')
     
     p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
     
     p.add_option('--include','-I',action='store',dest='include_paths',\
                 help="""Additional search paths for including modules 
                         with multiple entries separated by ;""")
     
     p.add_option('--output','-o',action='store',dest='output_directory',\
                 help='Output directory for task results')
     
     p.add_option('--ntaskCount','-n',action='store',type='int',default=0,\
             dest='max_task_count', help='Maximum number of tasks to perform')
     
     p.add_option('--silent','-s',action='store_true',\
             dest='silent', help='Suppress output of programs run by TaskExec')
             
     p.add_option('--sequential',action='store_true',\
              dest='sequential', help='Use sequential execution of tasks')

     p.add_option('--sshRun',action='store',\
             dest='ssh_run', help='Run the command remotely using ssh on a machine determined by the sshSubmit script')

     p.add_option('--local','-l',action='store_true',\
             dest='local_cache', help='A local cache of the task data base file, it it exists, will be used for input data.')

     p.add_option('--execID','-e',action='store',type='string',\
             dest='exec_id', help='Specify ID for TaskExec')
     
     p.add_option('--multiple','-m',action='store_true',\
             dest='multiple', help='Specifies a multiple instance invocation')
     
     p.add_option('--alternateTaskPrefix','-a',action='store',type='string',\
             dest='prefix_Task_ID', help='Specifies an alternate prefix to the task output file name. Default name is Task, resulting in output file names of the form Task_XXX.output.') 
     
     p.add_option('--filecoord','-f',action='store_true',\
             dest='file_coordination', help='Use file based task coordination, rather than db coordination.')
     
     p.add_option('--count','-c',action='store',type='int',default=0,\
             dest='count', help='Number of tasks in task database. If not specified, it will be determined by reading the task database.')
          
     options,arguments = p.parse_args()
    
     if(not(options.run_database)): 
         print('Run database file name must be specified')
         print('Specify using --database filename or -d filename ')
         exit()
     else:
         self.jobDBname = options.run_database
         
     if(options.task_table): 
         self.runTableName = options.task_table
     else:        
       self.runTableName = (os.path.basename(self.jobDBname).split("."))[0]

     if(options.include_paths):
         includePaths = options.include_paths.split(';')
         for i in range(len(includePaths)):
             os.sys.path.append(includePaths[i])
         os.sys.path.append('.')
       
     if(options.output_directory):
         self.alternateOutputDirectory = options.output_directory
     else:
         self.alternateOutputDirectory = '.'
        
     if(options.max_task_count):
         self.maxTaskCount = options.max_task_count
     else:
         self.maxTaskCount = 0
         
     if(options.count):
         self.totalTaskCount = options.count
     else:
         self.totalTaskCount = None         
       
         
     if(options.silent):
         self.silentRun = options.silent

     if(options.sequential):
         self.sequential = options.sequential
                 
     if(options.local_cache):
         self.localCache = options.local_cache
         
     if(options.multiple):
         self.multipleInstance = options.multiple
         
     if(options.file_coordination):
       self.fileBasedCoordination = True
       
     if(options.prefix_Task_ID):
       self.prefix_Task_ID = options.prefix_Task_ID
     else:
       self.prefix_Task_ID = None
         
     if(options.exec_id):
         self.exec_id = options.exec_id
     else:
       hostNameStart = gethostname().lower()
       hostName      = hostNameStart.split(".")[0]
       self.exec_id  = hostName
       
     if(options.ssh_run):
       self.ssh_run = options.ssh_run
     else: 
       self.ssh_run = None

 def setOptions(self, options):
   if(not('run_database' in options)): 
     print('Run database file name must be specified')
     print('Specify using --database filename or -d filename ')
     exit()
   else:
     self.jobDBname = options['run_database']
         
   if('ssh_run' in options):
     self.ssh_run = options['ssh_run']
   else:
      self.ssh_run = None
  
   if('task_table' in options): 
     self.runTableName = options['task_table']
   else:        
     self.runTableName = (os.path.basename(self.jobDBname).split("."))[0]

   if('include_paths' in options):
     includePaths = options['include_paths'].split(';')
     for i in range(len(includePaths)):
       os.sys.path.append(includePaths[i])
     os.sys.path.append('.')
       
   if('output_directory' in options):
     self.alternateOutputDirectory = options['output_directory']
   else:
     self.alternateOutputDirectory = '.'
        
   if('max_task_count' in options):
     self.maxTaskCount = options['max_task_count']
   else:
     self.maxTaskCount = 0
     
   if('prefix_Task_ID' in options):
     self.prefix_Task_ID = options['prefix_Task_ID']
   else:
     self.prefix_Task_ID = None

   if('count' in options):
      self.totalTaskCount = options['count']
   else:
     self.totalTaskCount = None  
                  
   if('silent' in options):
     self.silentRun = options['silent']
     
   if('sequential' in options):
     self.sequential = options['sequential']
     
   if('local_cache' in options):
         self.localCache = options['local_cache']
         
   if('multiple' in options):
     self.multipleInstance = options['multiple']
     
   if('file_coordination' in options):
     self.fileBasedCoordination = options['file_coordination']
          
   if('exec_id' in options):
     self.exec_id = options['exec_id']  
   else:
     hostNameStart = gethostname().lower()
     hostName      = hostNameStart.split(".")[0]
     self.exec_id  = hostName
   
#
# Import required modules. This routine must be called after the 
# parsing of the input data, as additional path are set based upon
# users input
#
 def importRequiredModules(self):
   try:
      from Classfetch import _get_func,_get_class    #class loader 
   except ImportError as exception:
      print('Failed to load required modules sqLite and Classfetch ') 
      print(exception)
      exit()
      
   # initialize class instances
   
   self._get_class       = _get_class
# 
# getTaskID 
#
# taskID         : An integer specifying the row of the job table of the task to be performed
# taskAvailable  : A flag indicating if there are any tasks available
# sqIDtailString : The tail end of the row selection string 
#                  = " WHERE ROWID=" + self.taskID + ';'
#  
 def getTaskID(self): 
 #
 # If a multiple instance run, then sleep a small amount of time 
 # to avoid conflicts at startup.
 #
  if(self.multipleInstance):
    sleepTime = self.taskConflictSleepFactor*random.random()
    time.sleep(sleepTime)
 # Fetch undone tasks and select a one at random from the list,
 # taking care not to select an already excluded task. 
 #
  execList = []
  taskList = []
  execValue = 0;
  if(self.fileBasedCoordination):
    execRunning = self.createFileLinesArray(self.execRunfileName)
    for i in execRunning:
      try:
         execValue = int(i.split(':')[0])
      except ValueError as e: 
         execValue = -1
      if(execValue != -1):
        execList.append(execValue)
        
        
    for i in range(1,self.totalTaskCount+1) :
      if ((not(i in execList)) and (not(i in self.taskExclusions))):
        taskList.append(i)
    
  else:
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level=None)
    sqDB   = sqCon.cursor()
    sqCommand = 'SELECT status from ' + self.runTableName + ";"
    self.executeDB(sqDB,sqCommand)
    taskListData = sqDB.fetchall()
    sqDB.close()
    sqCon.close()
  #
  # Create a list of available tasks, excluding those that have already
  # been associated with another TaskExec instance. 
  #
    taskList = []
    for i in range(0,taskListData.__len__()):
      ip1 = i+1
      if((taskListData[i][0] == 'task') and (not (ip1 in self.taskExclusions))):
        taskList.append(ip1)
    
  if(taskList.__len__() == 0):
    self.taskAvailable = False
    print(self.exec_id + ': all tasks complete ')
    return 
  #
  # Shuffle the task list to avoid collisions, and
  # then select a task that hasn't already been 
  # excluded.
  
  if(not self.sequential):
    random.shuffle(taskList)
  	
  taskVal = taskList[0]
      
  self.taskID = '%d'% taskVal

 
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
   self.executeDB(sqDB,sqCommand)
   dataFile     = sqDB.fetchall()[0][0]
   sqDB.close()
   sqCon.close()
   self.runTemplate = Template(dataFile.decode('utf8'))
   #
   # Treatment of the output handler:
   #
   # If database contains the output handler source it will subsequently
   # be extracted and compiled into the python environment. 
   #
   # If the database contains just the file name of the output handler,
   # then the class specified by the file is loaded. 
   #
   # If nothing is specified, then just return.
   #
   self.outputHandler     = None
   self.outputHandlerFlag = False
   try:
     outputHandlerName = self.jobData['outputHandlerClass_name']
   except KeyError as e:
     try: 
       outputHandlerScript = self.jobData['outputHandlerScript']
     except KeyError as e:
       return 
     #
     # Load the output handler from the specified file name
     #
     os.sys.path.append(os.path.dirname(outputHandlerScript))
     outputHandlerName  = os.path.basename(outputHandlerScript).split('.')[0]
     outputHandlerName  = outputHandlerName + '.' + outputHandlerName
     try:
       self.outputHandler          = _get_class(outputHandlerName)
       self.outputHandlerFlag = True
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
   # Output handler script is in the database, so extract it and compile it
   # into the local run environment
   #
   sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
   sqDB   = sqCon.cursor()
  
   handlerData     = 'outputHandlerScript_data'
   sqCommand = "SELECT " + handlerData + '  FROM '  + self.runTableName + "_support where rowid = 1; "
   self.executeDB(sqDB,sqCommand)
   dataFileTmp           = sqDB.fetchall()[0][0]
   sqDB.close()
   sqCon.close()
   
   # Pythone2.7 -> Python3 mod
   #outputHandlerDataFile = StringIO.StringIO(buffer(dataFileTmp))
   outputHandlerDataFile = StringIO(dataFileTmp)
   #
   # Create and add the output data handler as a module.
   # The source code is stored as a string in the database. 
   #
   try:
     dataFile = outputHandlerDataFile.read().replace('\r', '')
     # Pythone2.7 -> Python3 mod
     #new.module(str(outputHandlerName))
     module = ModuleType(bytes(outputHandlerName))
     exec (dataFile in module.__dict__)
     sys.modules[bytes(outputHandlerName)] = module
     self.outputHandler = getattr(module,bytes(outputHandlerName))
     self.outputHandlerFlag = True
   except AttributeError as exception:
     print('Failed to load class  ' + outputHandlerName)
     print(exception)
     exit()

     
 def createRunDataFile(self):
  #
  # Substitute in the parameters
  #
  substituteData = {}
  for i in range(len(self.paramKeys)):
    substituteData[self.paramKeys[i]] = (self.substituteVal(self.runData[self.paramKeys[i]],self.runDataType[self.paramKeys[i]]))
  self.runDataFile =  self.runTemplate.substitute(substituteData)
  #self.runDataFile =  self.runTemplate.substitute(self.runData)
  #self.runFileName =  self.runTableName + '_' + self.taskID + '.qdt'
  self.runFileName  = self.jobData['runFileName']
  fName       =  self.workDirName + os.path.sep + self.runFileName
  
  self.writeToUnixFile(bytes(self.runDataFile, encoding='utf8'), fName)
  
  #
  # create job data support input files 
  # 
  fileNames = self.jobData['fileNames'].split(":")
  for i in fileNames:
    if((i != 'outputHandlerClass') \
    and (i != 'outputHandlerScript') \
    and (i != 'runFileTemplate') \
    and (i != 'xmlTaskFile')):
      fName = self.workDirName + os.path.sep + os.path.basename(self.jobData[i + '_name'])
      #python2.7 -> python3
      #fileHandle   = StringIO.StringIO(buffer(self.jobData[i + '_data']))
      fileHandle   = StringIO(self.jobData[i + '_data'].decode('utf-8'))
      dataFileTmp  = fileHandle.read() 
      fileHandle.close()
      self.writeToUnixFile(dataFileTmp , fName)

 def writeExecIDtoTaskIDfile(self):
   self.taskIDfileName =  self.workDirName + os.path.sep + '.taskID' 
   if (os.sys.platform == 'win32'):
     f = open(self.taskIDfileName, "ab")
     f.write(bytes(self.exec_id + "\n", encoding='utf8'))
     f.close()    
   else:
     f = open(self.taskIDfileName, "ab")
     f.write(bytes(self.exec_id +"\n", encoding='utf8'))
     f.close()   
     
 def writeExecIDtoDoneIDfile(self):
   self.doneIDfileName  = os.path.abspath(self.alternateOutputDirectory) + os.path.sep + '.doneID'
   if (os.sys.platform == 'win32'):
     f = open(self.doneIDfileName, "ab")
     f.write(bytes(self.exec_id + "\n", encoding='utf8'))
     f.close()    
   else:
     f = open(self.doneIDfileName, "ab")
     f.write(bytes(self.exec_id +"\n",encoding='utf8'))
     f.close()  
     
 def writeToExecRunFile(self):
  if (os.sys.platform == 'win32'):
    f = open(self.execRunfileName, "ab")
    f.write(bytes(self.taskID + ':' + self.exec_id + "\n",encoding='utf8'))
    f.close()    
  else:
    f = open(self.execRunfileName, "ab")
    f.write(bytes(self.taskID + ':' + self.exec_id +"\n",encoding='utf8'))
  
    f.close() 
 
 def writeToExecDoneFile(self):
  if (os.sys.platform == 'win32'):
    f = open(self.execDonefileName, "ab")
    f.write(bytes(self.taskID + ':' + self.exec_id + "\n",encoding='utf8'))
    f.close()    
  else:
    f = open(self.execDonefileName, "ab")
    f.write(bytes(self.taskID + ':' + self.exec_id +"\n",encoding='utf8'))
    f.close()     

      
 def createRunWorkingDirectory(self):
  if (not (os.path.isdir(self.alternateOutputDirectory))): os.mkdir(self.alternateOutputDirectory)
  #
  # Write out the data file to the run directory 
  #
  self.taskIDout = self.taskID
  if   (int(self.taskID) <= 9)  : self.taskIDout = '00' + self.taskID
  elif (int(self.taskID) <= 99) : self.taskIDout = '0'  + self.taskID
  else                          : self.taskIDout = self.taskID
  
  self.workDirName =  self.alternateOutputDirectory + os.path.sep + self.runTableName + '_' + self.taskIDout
  if (not (os.path.isdir(self.workDirName))) : os.mkdir(self.workDirName)
  
  #


  
 def handleProgramOutput(self): 
    if(not self.outputHandlerFlag): 
      return
    outputLines = self.runOutput.splitlines()
    handlerArgs = {}
    handlerArgs['outputLines']   = outputLines
    handlerArgs['jobData']       = self.jobData
    handlerArgs['taskData']      = self.taskData
    handlerArgs['runData']       = self.runData
    handlerArgs['outputData']    = self.outputData
    handlerArgs['jobDBname']     = self.jobDBname
    handlerArgs['runTableName']  = self.runTableName
    handlerArgs['taskID']        = self.taskID
    handlerArgs['execDirectory'] = self.localDirectory
    handlerArgs['outputDataType']= self.outputDataType
    self.oHandler = self.outputHandler(handlerArgs)
    #
    # Use the handler class to fill the output data 
    #
    self.oHandler.fillOutputData(outputLines,self.taskData,self.jobData,self.runData,self.outputData)


 def packOutput(self): 
  #
  # Pack output data into the database
  #
  #      jobData['taskDate']  = time.asctime()
  #      jobData['taskTime']  = ''    jobData['taskTime']                = ''

  if(self.multipleInstance):
    sleepTime = self.minimalTaskTime*random.random()
    time.sleep(sleepTime)
    
  jData = " SET status = \'done\' "
  jData = jData + ", exec_id = " + "\'" + self.exec_id  + "\'"
  jData = jData + ', taskDate = ' + self.insertVal(time.asctime(),'TEXT')
  jData = jData + ', taskTime = ' + self.insertVal(self.taskTime,'REAL') + ' ' 
  sqCommand = 'UPDATE ' + self.runTableName + jData
  for i in range(len(self.outputKeys)):
    val     = self.outputData[self.outputKeys[i]]
    valType = self.getSQLiteType(val)
    if(valType != "BLOB"):
      sqCommand = sqCommand +  ', ' + self.outputKeys[i] + '=' + self.insertVal(val,valType)

  sqCommand = sqCommand +  self.sqIDtailString
    
    
    #sqCommand  = 'UPDATE ' + self.runTableName + " SET status = \'done\' " + self.sqIDtailString;
    
    #
    # Connect to database and update with output. Induce use of sqLite3's 
    # autocommit mode by setting isolation_level to None
    #

  sqCon  = sqlite3.connect(self.jobDBname,isolation_level=None)
  sqDB   = sqCon.cursor()

  self.executeDB(sqDB,sqCommand)
    
  sqDB.close()
  sqCon.close()

   
 def finalize(self):
#
# Check to see if this is the last task, if so then call the finalize 
# member function of the output handler. 
#
  if(not self.outputHandlerFlag): 
    return
  
  if(self.fileBasedCoordination):
    doneRunning   = self.createFileLinesArray(self.execDonefileName)
    taskToDo      = int(doneRunning[0].split(':')[1])
    taskDoneCount = 0
    for i in doneRunning:
      if((i.split(":")[0] != 'TasksToBeDone') and (i != '')): 
        taskDoneCount = taskDoneCount + 1
        
    if(taskDoneCount != taskToDo):
      return
  
  else:
    sqCon    = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB     = sqCon.cursor()
    sqCommand = "SELECT count(status) FROM " + self.runTableName + " WHERE status = 'done'; "
    self.executeDB(sqDB,sqCommand)
    currentDoneCount = sqDB.fetchall()[0][0]
    sqDB.close()
    sqCon.close()
    if(self.totalTaskCount != currentDoneCount):
      return
  
  if(self.multipleInstance):
    self.writeExecIDtoDoneIDfile()
  
  if(self.multipleInstance):
    taskIDfile     = open(self.doneIDfileName, "r")
    firstCaptureID = taskIDfile.readline()
    taskIDfile.close()
    if(firstCaptureID.strip() != self.exec_id.strip()):
      return

  handlerArgs = {}
  handlerArgs['jobData']       = self.jobData
  handlerArgs['taskData']      = self.taskData
  handlerArgs['runData']       = self.runData
  handlerArgs['outputData']    = self.outputData
  handlerArgs['jobDBname']     = self.jobDBname
  handlerArgs['runTableName']  = self.runTableName
  handlerArgs['taskID']        = self.taskID
  handlerArgs['execDirectory'] = self.localDirectory
  handlerArgs['totalTaskCount']= self.totalTaskCount
  handlerArgs['outputDataType']= self.outputDataType
  
  self.oHandler.finalize(handlerArgs)
 
#
# captures jobKeys, jobData, outputKeys,outputData, paramKeys, runData
#
 def captureTaskData(self):
  #
  # Caputure the task attributes 
  #
  self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
  #
  # If the local cache flag is on, then specify create the full path name to it.
  # Reads of the task data will be from the local cache, rather than the originating
  # database. 
  #
  localDBname = None
  if(self.localCache):
    localDbName =    os.path.abspath(self.workDirName) + os.path.sep + 'local_' + os.path.basename(self.jobDBname)
    if(not os.path.exists(localDbName)):
      localDbName = None
  #
  # On initial pass capture the input template file and output handler 
  #
  if(self.firstCall):
    self.firstCall = False
  
    if(localDBname == None):
      sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    else:
      sqCon  = sqlite3.connect(localDbName,isolation_level = None)
      
    sqDB   = sqCon.cursor()
  
    sqCommand = 'SELECT jobDataNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
    self.executeDB(sqDB,sqCommand)
    jobDataNames =  sqDB.fetchall()[0]
    self.jobKeys = jobDataNames[0].split(':')
    
    for i in range(len(self.jobKeys)):
      sqCommand  = 'SELECT ' + self.jobKeys[i] + ' from ' + self.runTableName + '_support  WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      self.jobData[self.jobKeys[i]]=sqDB.fetchone()[0]
      
    sqCommand              = 'SELECT outputDataNames from ' + self.runTableName + '_support WHERE ROWID = 1;'
    self.executeDB(sqDB,sqCommand)
    outputDataNames   = sqDB.fetchall()[0]
    self.outputKeys    = outputDataNames[0].split(':');
    
    for i in range(len(self.outputKeys)):
      sqCommand  = 'SELECT typeof(' + self.outputKeys[i] + ') from ' + self.runTableName + ' WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      self.outputDataType[self.outputKeys[i]] = sqDB.fetchone()[0]
    
    sqCommand              = 'SELECT taskDataNames from ' + self.runTableName + '_support WHERE ROWID = 1;'
    self.executeDB(sqDB,sqCommand)
    taskDataNames   = sqDB.fetchall()[0]
    self.taskKeys   = taskDataNames[0].split(':');
    
    sqCommand              = 'SELECT runParameterNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
    self.executeDB(sqDB,sqCommand)
    runParameterNames = sqDB.fetchall()[0]
    self.paramKeys         = runParameterNames[0].split(':');
    
    for i in range(len(self.paramKeys)):
      sqCommand  = 'SELECT typeof(' + self.paramKeys[i] + ') from ' + self.runTableName + ' WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      self.runDataType[self.paramKeys[i]] = sqDB.fetchone()[0]

    sqDB.close()
    sqCon.close()
    
    self.importTemplateFileAndOutputHandler()
  #
  # Standard call 
  #
  if(localDBname == None):
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
  else:
    sqCon  = sqlite3.connect(localDbName,isolation_level = None)
    
  sqDB   = sqCon.cursor()
    
  for i in range(len(self.taskKeys)):
    sqCommand  = 'SELECT ' + self.taskKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
    self.executeDB(sqDB,sqCommand)
    self.taskData[self.taskKeys[i]] = sqDB.fetchone()[0]
           
  for i in range(len(self.outputKeys)):
    sqCommand  = 'SELECT ' + self.outputKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
    self.executeDB(sqDB,sqCommand)
    self.outputData[self.outputKeys[i]] = sqDB.fetchone()[0]
  
  for i in range(len(self.paramKeys)):
    sqCommand  = 'SELECT ' + self.paramKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
    self.executeDB(sqDB,sqCommand)
    self.runData[self.paramKeys[i]] = sqDB.fetchone()[0]

  sqDB.close()
  sqCon.close()
  
#
#===========      createFileLinesArray  ============================
#      
 def createFileLinesArray(self,fileName):
  try :
    f = open(fileName,'r')
  except IOError as exception:
    print('   === Error ===')
    print(" Data file cannot be read") 
    print(exception)
    exit()
      
  fileContents =  f.read()
  f.close()
  #
  # Verify that the data file is of Unix format 
  # (using construct from crlf.py in Toos/sripts)
  #
  if(fileContents.find("\r\n")):
    newContents = fileContents.replace("\r\n", "\n")
    fileContents = newContents

  fileLines = fileContents.split("\n");
  return fileLines

 def executeDB(self,sqDB,sqCommand):
   tryCount = 0
   tryMax   = 100
   errFlag  = True
   conflictTimeFactor = self.readDBconflictStartingFactor
   while(tryCount < tryMax):
     try :
       sqDB.execute(sqCommand)
       tryCount = tryMax
       errFlag = False
     except sqlite3.OperationalError as e:
       tryCount = tryCount +1
       #if(e[0].find("no such table") != -1):
       #  print(e)
       #  exit()
       print(e)
       print("Database locked. Retrying ...")
       sleepTime = conflictTimeFactor*random.random()
       conflictTimeFactor = 2.0*conflictTimeFactor
       if(conflictTimeFactor > self.readDBconflictBoundingFactor):
         conflictTimeFactor = self.readDBconflictBoundingFactor
       time.sleep(sleepTime)
   if(errFlag):
      print("TaskExec unable to write to database. Stopping task process")
      print("TaskExec command : "  + sqCommand)
      exit()
   
 def insertVal(self,val,valType):
     if(valType == 'TEXT'):    return  "\'" + val + "\'"
     if(valType == 'INTEGER'): return '%d'      % val
     if(valType == 'REAL'):    return '%-.16e'  % val
     if(valType == 'BLOB'):    return val


 def substituteVal(self,val,valType):
     if(valType == 'text'):    return val
     if(valType == 'integer'): return '%d'      % val
     if(valType == 'real'):    return '%-.16e'  % val
     if(valType == 'blob'):    return val
     
 def getSQLiteType(self,val):
     if(type(val) is int    ): return 'INTEGER'
     if(type(val) is bytes  ): return 'TEXT' 
     if(type(val) is str    ): return 'TEXT' 
     if(type(val) is float  ): return 'REAL'
     return 'BLOB'
   
 def getSQLiteValue(self,val):
     if(type(val) is int    ): return '%d'      % val
     if(type(val) is bytes  ): return  "\'" + val + "\'" 
     if(type(val) is str    ): return  "\'" + val + "\'" 
     if(type(val) is float  ): return '%-.16e'  % val
     return val
   
 def importSSHsubmitClass(self,sshSubmitClassName):
    try:
      from classfetch import _get_func,_get_class    #class loader 
    except ImportError as exception:
      print('Failed to load required modules classfetch ') 
      print(exception)
      exit()
    
    os.sys.path.append(os.path.dirname(sshSubmitClassName))
    sshClassName = os.path.basename(sshSubmitClassName).split('.')[0]
    sshClassName  = sshClassName + '.' + sshClassName
    try:
      sshSubmitClass = _get_class(sshClassName)
      return sshSubmitClass
    except ImportError as exception:
      print('Failed to load class ' + sshClassName)
      print(exception)
      exit()
    except AttributeError as exception:
      print('Failed to load class ' + sshClassName)
      print(exception)
      exit()
#
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
  taskExec = TaskExec()
  taskExec.parseStandardOptions()
  taskExec.run()


                 





