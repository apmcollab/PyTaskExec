from __future__ import print_function
import optparse
import subprocess
import os
import time
import random
from socket import gethostname
import sys
from random import Random
from shutil import copy

import threading
try:
    import queue
except ImportError:
    import Queue as queue

from TaskExecThread import TaskExecThread


#  
# Changes: 5/13/08-
# Created random name extensions so that TaskExec's spawned 
# by multiple invocations of ExecRun will be distinct. Also, 
# an ExecRun can be restarted (it will begin working on any 
# tasks with status "task") and multiple instances of ExecRun
# can be started. 
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

class ExecRun(object):
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
    
    self.tasksToBeDone  = 0
    
  def getFileFullPath(self,fileName):
    if(os.path.isfile(fileName)): 
      return os.path.abspath(fileName)
  
    for i in range(len(os.sys.path)):
      fullPathName = os.sys.path[i] + os.path.sep + fileName
      if(os.path.isfile(fullPathName)): 
        return os.path.abspath(fullPathName)
    
    print('                 === Error ===')
    print(fileName + " not found") 
    print(exception)
    exit() 

  def importBatchSubmitClass(self,batchClassName):
    try:
      from Classfetch import _get_func,_get_class    #class loader 
    except ImportError as exception:
      print('Failed to load required modules Classfetch ') 
      print(exception.message)
      exit()
    
    os.sys.path.append(os.path.dirname(batchClassName))
    outputClassName = os.path.basename(batchClassName).split('.')[0]
    outputClassName  = outputClassName + '.' + outputClassName
    try:
      batchClass = _get_class(outputClassName)
      return batchClass
    except ImportError as exception:
      print('Failed to load class ' + outputClassName)
      print(exception.message)
      exit()
    except AttributeError as exception:
      print('Failed to load class ' + outputClassName)
      print(exception.message)
      exit()
  
      
  def prepareWorkingDirectories(self,outputDirectory,runTableName,taskCount,taskList):
    
    if (not (os.path.isdir(outputDirectory))): 
      os.mkdir(outputDirectory)
    
    doneIDfileName  =  os.path.abspath(outputDirectory) + os.path.sep + '.doneID'
    execRunfileName =  os.path.abspath(outputDirectory) + os.path.sep + '.execTask'
    doneRunfileName =  os.path.abspath(outputDirectory) + os.path.sep + '.doneTask'
    
    if(os.path.exists(doneIDfileName)):
      os.remove(doneIDfileName)
    if(os.path.exists(execRunfileName)):
      os.remove(execRunfileName)
    if(os.path.exists(doneRunfileName)):
      os.remove(doneRunfileName)

    #
    # Write the number of tasks that are to be done 
    #
    if (os.sys.platform == 'win32'):
      f = open(doneRunfileName, "ab")
      f.write('TasksToBeDone:' + str(self.tasksToBeDone) + "\n")
      f.close()    
    else:
      f = open(doneRunfileName, "a")
      f.write('TasksToBeDone:' + str(self.tasksToBeDone) + "\n")
      f.close()
    #
    # Create working directories if required. If a working directory
    # exists then remove the existing .taskID file. Also if the local cache flag
    # is set, then make a local copy of the task database to read task input
    # from. 
    #
    for i in range(1,taskCount + 1):
      taskIDout = '0'
      if   (int(i) <=  9) : 
        taskIDout = '00' + '%d' % i
      elif (int(i) <= 99) : 
        taskIDout = '0'  + '%d' % i
      else               : 
        taskIDout = '%d' % i
      workDirName =  outputDirectory + os.path.sep + runTableName + '_' + taskIDout

      if (not (os.path.isdir(workDirName))) : 
        os.mkdir(workDirName)
        localCacheName = os.path.abspath(workDirName) + os.path.sep + 'local_' + os.path.basename(self.standardOptions['runDBname']);
        if(self.standardOptions['localCache']):
          copy(self.standardOptions['runDBname'],localCacheName)
      else:
        localCacheName = os.path.abspath(workDirName) + os.path.sep + 'local_' + os.path.basename(self.standardOptions['runDBname']);
        taskIDfile=workDirName + os.path.sep + '.taskID' 
        if((os.path.isfile(taskIDfile)) and (i in taskList)):
          os.remove(taskIDfile)
        if((self.standardOptions['localCache']) and (i in taskList)):
           copy(self.standardOptions['runDBname'],localCacheName)
        else:
          if(os.path.isfile(localCacheName)):
            os.remove(localCacheName)

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
    runTableName = self.standardOptions['runTableName'] 
    try :
      sqCon        = sqlite3.connect(self.standardOptions['runDBname'],isolation_level=None)
      sqDB         = sqCon.cursor()
      sqCommand    = 'SELECT status FROM ' + runTableName + ';' 
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
        
    if(self.standardOptions['nExec'] > self.tasksToBeDone):
      self.standardOptions['nExec'] = self.tasksToBeDone
    #
    # Prepare the working directories by creating them if they don't exist.
    # If the working directory already exists, then remove the .taskID file
    # and also copy in a new version of the task table if the local cache flag
    # is one. 
    #
    if(not self.standardOptions['multipleInstance']):
      self.prepareWorkingDirectories(self.standardOptions['outputDir'],runTableName,taskCount,taskList)
    #
    #   Find the path to the python program
    #
    if(self.standardOptions['pythonProgram'] != ''):
      pythonCommand = self.standardOptions['pythonProgram']
    else:
      if os.sys.platform == 'win32': 
        pythonExeName = 'python.exe'
        for i in range(len(os.sys.path)):
            pythonProg = os.sys.path[i] + os.path.sep + pythonExeName
            if(os.path.isfile(pythonProg)): 
              pythonCommand = pythonProg
      else:
        pythonCommand= (subprocess.Popen('which python3',shell=True,stdout=subprocess.PIPE).communicate()[0]).decode(encoding='UTF-8')
        pythonCommand= pythonCommand.replace('\n','')
        if(not os.path.isfile( pythonCommand)):
          pythonCommand= (subprocess.Popen('which python',shell=True,stdout=subprocess.PIPE).communicate()[0]).decode(encoding='UTF-8')
          pythonCommand= pythonCommand.replace('\n','')
          if(not os.path.isfile( pythonCommand)):
            print('full path to python command not Found. ')
            print('Search returned : ' + pythonCommand) 
            print('   === Program Halted ===')
            exit() 
    #
    #  Find the path to the TaskExec.py 
    #
    for i in range(len(os.sys.path)):
      execProg = os.sys.path[i] + os.path.sep + 'TaskExec.py'
      if(os.path.isfile(execProg)):
         break
    #
    # If not using a batch submission program,
    #  then start the TaskExec's in separate threads and run them.
    #
    localDir = os.getcwd()
    if(self.standardOptions['batch_submit'] == ''):
    #
    # Create run command using the full path to the python command 
    #   
    # print( type(self.standardOptions['runTableName']))
      runCommand = pythonCommand + ' ' + execProg  + ' -d ' + self.standardOptions['runDBname'] \
                                               + ' -t ' + self.standardOptions['runTableName']
      if(self.standardOptions['outputDir'] != ''): 
        runCommand = runCommand + ' -o ' + self.standardOptions['outputDir']
      if(self.standardOptions['includePaths'] != ''): 
        runCommand = runCommand + ' -I ' + self.standardOptions['includePaths']  
      if(self.standardOptions['silent']):
        runCommand = runCommand + ' -s ' 
      if(self.standardOptions['prefix_Task_ID']):
        runCommand = runCommand + ' -a ' + self.standardOptions['prefix_Task_ID']
    
    # add option for multiple instance invocations and local cache 
    
      runCommand = runCommand + ' -m '
      
      if(self.standardOptions['localCache']):
        runCommand = runCommand + ' -l ' 
        
    # add option for file based task coordination
    
      if(self.standardOptions['fileBasedCoordination']):
         runCommand = runCommand + ' -f ' 
         
    # specify the total task count (avoids the read of the data base by the TaskExec)
    
      runCommand = runCommand + ' -c ' + str(taskCount)
#
#   Start up task processing with a separate id for each TaskExec instance.
#
#   Each instance is started in a separate thread in which the TaskExec.py
#   is executed in a spawned process with the P_WAIT flag set. 
#
#   When the spawned process returns, the thread posts a response to the
#   to the response queue. The main thread is blocked by the response_queue.get()
#   call until TaskExec threads have posted. 
#   
      try:
        response_queue = Queue.Queue()
      except NameError:
        response_queue = queue.Queue()
      
      runCommand = runCommand.split(' ')
      runCommand.append('-e')  
      lastIndex  = len(runCommand)
      runCommand.append('') 
      
      for i in range(1,int(self.standardOptions['nExec'])+1):
        print("Starting Exec # ", i)
        tmpSuffix     =  self.getRandomName()
        runCommand[lastIndex]  = self.standardOptions['exec_id'] + '_%d'%i + "_" + tmpSuffix 
        TaskExecThread(i,response_queue,pythonCommand,runCommand).start()
        #os.spawnv(os.P_NOWAIT,pythonCommand,runCommand)
        print("Executing: ", end=' ')
        for j in range(len(runCommand)):
            print(runCommand[j], end=' ')
        print(' ')
      #
      # Waiting for all threads to post a response 
      #
      for i in range(1,int(self.standardOptions['nExec'])+1):
          response_queue.get()
      #
      #  
      print("XXXXXXXXX ExecRun Finished XXXXXXXXXXXXXX") 
#
# Using a batch submission program
#       
    if(self.standardOptions['batch_submit']):
      dBname    = self.getFileFullPath(self.standardOptions['runDBname'])
      try:
        outputDir = self.getDirFullPath(self.standardOptions['outputDir'])
      except :
        outputDir = localDir + os.path.sep + self.standardOptions['outputDir']
        os.mkdir(outputDir)
    
      runCommandBase = ' ' + execProg  + ' -d ' +  dBname + ' -t ' + self.standardOptions['runTableName']
      if(self.standardOptions['outputDir'] != ''): 
        runCommandBase = runCommandBase + ' -o ' + outputDir
      if(self.standardOptions['includePaths'] != ''): 
        runCommandBase = runCommandBase + ' -I ' + self.standardOptions['includePaths']  
      if(self.standardOptions['silent']):
        runCommandBase = runCommandBase + ' -s ' 
      if(self.standardOptions['prefix_Task_ID']):
        runCommandBase= runCommandBase + ' -a ' + self.standardOptions['prefix_Task_ID']
    
      runCommandBase = runCommandBase + ' -m '
      if(self.standardOptions['localCache']):
        runCommandBase = runCommandBase + ' -l ' 
        
      if(self.standardOptions['fileBasedCoordination']):
         runCommandBase = runCommandBase + ' -f ' 
         
    # specify the total task count (avoids the read of the data base by the TaskExec)
    
      runCommandBase= runCommandBase + ' -c ' + str(taskCount)
      if(not self.standardOptions['silent']):
        print(runCommandBase)
    #
    # Read in batch submission template
    #
      batchSubmissionClass = self.getFileFullPath(self.standardOptions['batch_submit'])
      batchClass   = self.importBatchSubmitClass(batchSubmissionClass)
      batchProgram = batchClass(os.path.dirname(batchSubmissionClass))
      
      threadStartFlag = False
      try:
        threadStartFlag =  batchProgram.threadStartFlag
      except AttributeError:
        threadStartFlag = False
    #
    # Set up directories for Exec's, create a cmd file, and submit the
    # job to the cluster
    #  
      if(threadStartFlag):
        response_queue = Queue.Queue()
        
      for i in range(1,int(self.standardOptions['nExec'])+1):
        uffix     =  self.getRandomName()
        execDir = outputDir + os.path.sep + self.standardOptions['exec_id'] + '_%d'%i + "_" + tmpSuffix 
        try:
          execDir = self.getDirFullPath(execDir)
        except :
          os.mkdir(execDir)
    #
    #  Clean up the exec directory 
    #   
        execFileList = os.listdir(execDir)
        for eFile in execFileList:
          removeFileName = execDir + os.path.sep + eFile
          os.remove(removeFileName )
    #
    #   Prepare batch submission script 
    #
        execCommandInfo = {}
        execCommandInfo['execIndex']      = i
        execCommandInfo['execName']       = os.path.basename(execDir)
        execCommandInfo['execWorkingDir'] = execDir
        execCommandInfo['runCommand']     = runCommandBase
        execCommandInfo['pythonCommand']  = pythonCommand
    
        if(threadStartFlag == False):
          batchProgram.createAndSubmitBatchCmdFile(execCommandInfo)
        else:
          batchProgram.createAndSubmitBatchCmdFile(execCommandInfo,response_queue)
            
      if(threadStartFlag == True):
      #
      # Waiting for all submit threads to post a response 
      #
        for submitIndex in range(1,int(self.standardOptions['nExec'])+1):
            response_queue.get()
        print("XXXXXXXXX ExecRun Finished XXXXXXXXXXXXXX") 
        
  def getRandomName(self):
    rng = Random()
    righthand      = '23456qwertasdfgzxcvbQWERTASDFGZXCVB'
    lefthand       = '789yuiophjknmYUIPHJKLNM'
    allchars       = righthand + lefthand
    passwordLength = 8
    name = rng.choice(allchars)
    for i in range(passwordLength-1):
      name = name + rng.choice(allchars)
    return name
#
#===========     End Main Program   ============================
#  

#
#   Stub for executing the class defined in this file 
#   
if __name__ == '__main__':
  execRun = ExecRun()
  execRun.parseStandardOptions()
  execRun.run()
