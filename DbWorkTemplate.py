from __future__ import print_function
import sqlite3
import os
import optparse
import random
import time


class DbWorkTemplate(object):
  """
  A routine that executes a particular Python task for
  each task in a specified task database. This routine differs
  from a python output handler in that it is run sequentially 
  from the command line (e.g. not executed by ExecRun). 
  
  Currently all this template does is to print the list of 
  the directories associated with the particular task table. 
  
  Required parameters 
  
    (-d) --database  : Task database
    
    (-t) --tasktable : Table containing task data (if not default)
   
    (-o) --output    : Output directory containing task directories
     
  (C) UCLA Chris Anderson 2009
  """
  def __init__(self):
    self.standardInputs = {}
    self.standardInputs['taskDBname']        =  None
    self.standardInputs['taskTableName']     =  None
    self.standardInputs['taskDir']           =  None


  def parseOptions(self):
    p = optparse.OptionParser()
    p.add_option('--database','-d',action='store',dest='task_database',\
                 help='Specifies job task database file (required)')
    
    p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')

    p.add_option('--output','-o',action='store',dest='task_directory',\
                 help='Output directory containing task results')
  
    
    options,arguments = p.parse_args()
    
    if(not(options.task_database)): 
      print('Task database file name must be specified')
      print('Specify using --database filename or -d filename ')
      exit()
    else:
     self.standardInputs['taskDBname'] = options.task_database
     
    if(options.task_table): 
      self.standardInputs['taskTableName']  = options.task_table
    else:        
      self.standardInputs['taskTableName']  = (os.path.basename(self.standardInputs['taskDBname']).split("."))[0]
     
    if(options.task_directory):
      self.standardInputs['taskDir'] = options.task_directory
    else:
      self.standardInputs['taskDir'] = '.'  
      
#
# The main event loop 
#    
  def run(self):
    
    workDirName = os.getcwd()
    #
    # check to see if the requisite directories exist
    #
    self.getTaskList()
    for i in self.taskList:
      self.taskID = str(i)
      self.getRunWorkingDirectory()
      
    # self.TaskList = list of tasks
    #
    # To access each directory:
    #
    # Set the self.TaskID to a task
    # Call self.getRunWorkingDirectory() 
    #
    # self.workDirName =  working directory name
    # self.taskIDout   =  the task output file name index (e.g. 001, 011, 111) 

    for k in range(1,self.taskList.__len__()+1):
      self.taskID = str(k)
      self.getRunWorkingDirectory()
      print("Task " + self.taskID + " "  + self.workDirName) 


  def getTaskList(self):
    sqCon  = sqlite3.connect(self.standardInputs['taskDBname'],isolation_level = None)
    sqDB   = sqCon.cursor()
    sqCommand = 'select rowid from ' + self.standardInputs['taskTableName'] + ";"
    sqDB.execute(sqCommand)
    taskListTmp = sqDB.fetchall()
    sqDB.close(); sqCon.close()
    self.taskList = []
    for i in range(0,taskListTmp.__len__()):
      self.taskList.append(taskListTmp[i][0])
    
  def getFullPath(self, fileName):
    if(os.path.isfile(fileName)):
      return os.path.abspath(fileName)
    else:
      print('                 === Error ===')
      print(" File not found") 
      print('\'' + fileName + '\'')
      exit() 
      
  def getRunWorkingDirectory(self):
    if (not (os.path.isdir(self.standardInputs['taskDir']))): 
      print('Error: Job Output Directory \''+ self.standardInputs['taskDir'] \
      +'\' Doesn\'t Exist')
      exit();

    self.taskIDout = self.taskID
    if   (int(self.taskID) <= 9)  : 
      self.taskIDout = '00' + self.taskID
    elif (int(self.taskID) <= 99) : 
      self.taskIDout = '0'  + self.taskID
    else                          : 
      self.taskIDout = self.taskID
  
    self.workDirName =  self.standardInputs['taskDir'] + os.path.sep + self.standardInputs['taskTableName']  \
    + '_' + self.taskIDout
    if (not (os.path.isdir(self.workDirName))) : 
      print('Error: Job Output Directory \''+ self.workDirName \
      +'\' Doesn\'t Exist')
      exit();

  def getDataFile(selfself,fileName):
    try :
      f = open(fileName,'rU')
    except IOError as exception:
      print('                 === Error ===')
      print(" file " + fileName +  " cannot be read") 
      print(exception)
      exit()
      
    fileContents =  f.read()
    f.close()
    return fileContents
    
#
#===========      getPathFromOSpath ============================
# 
  def getPathFromOSpath(self,fileName):
    sysPath = os.sys.path
    for i in sysPath:
      fileNameNew = i + os.path.sep + fileName
      if(os.path.isfile(fileNameNew)):
        return fileNameNew
    return None   

#===========      writeToUnixFile  ============================
#   
  def writeToUnixFile(self,fileContents,fileName):
    if os.sys.platform == 'win32':
      outputFile = fileContents.replace("\r\n", "\n")
      f = open(fileName, "wb+")
      f.write(outputFile)
      f.close()    
    else:
      f = open(fileName, "wb+")
      f.write(fileContents)
      f.close() 
#

  def executeDB(self,sqDB,sqCommand):
   tryCount = 0
   tryMax   = 100
   while(tryCount < tryMax):
     try :
       sqDB.execute(sqCommand)
       tryCount = tryMax
     except sqlite3.OperationalError as e:
       tryCount = tryCount +1
       print("Database locked. Retrying ...")
       sleepTime = .01*random.random()
       time.sleep(sleepTime)
#
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
    dbWorkTemplate = DbWorkTemplate()
    dbWorkTemplate.parseOptions()
    dbWorkTemplate.run()

