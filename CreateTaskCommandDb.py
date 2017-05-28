from __future__ import print_function
import sqlite3
import os
import optparse
import random
import time
from TaskDbBuilder import TaskDbBuilder
from string import Template


class CreateTaskCommandDb(object):
  """
  A routine that creates a task database of a specified command 
  to be executed for the output associated with each task in 
  an existing task database. 
  
  Required parameters 
  
    (-d) --database  : Task database
    
    (-t) --tasktable : Table containing task data (if not default)
   
    (-o) --output    : Output directory containing task directories
    
    (-c) --command   : Command to be executed for each task 
      
    (-n) --name      : Name of task database created
    
    (-s) --script    : Name of output handler script to use 
                 
  """
  def __init__(self):
    self.taskDbBuilder = TaskDbBuilder()
    self.standardInputs = {}
    self.standardInputs['taskDBname']        =  None
    self.standardInputs['taskTableName']     =  None
    self.standardInputs['taskDir']           =  None
    self.standardInputs['command']           =  None
    self.standardInputs['cmdTaskDb']         =  None
    self.standardInputs['handlerScript']     =  None

  def parseOptions(self):
    p = optparse.OptionParser()
    p.add_option('--database','-d',action='store',dest='task_database',\
                 help='Specifies job task database file (required)')
    
    p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
      
    p.add_option('--command ','-c',action='store',dest='command',\
                 help='Command to be executed for each task')
        
    p.add_option('--name','-n',action='store',dest='command_database',\
                 help="""Name for command database. Default  
                         TaskCMD.db""")
    
    p.add_option('--output','-o',action='store',dest='task_directory',\
                 help='Output directory containing task results')
  
    p.add_option('--script','-s',action='store',dest='handler_script',\
                 help='Handler script for processing std output')
    
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
         
    if(not(options.command)): 
      print('Command  must be specified')
      print('Specify using --command filename or -c command ')
      exit()
    else:
      self.standardInputs['command']  = options.command
     
    if(options.command_database):
      if(self.standardInputs['taskDBname'].strip() == options.command_database.strip()):
        print('Command database file name must differ from Task database file name.')
        print('Script terminated.')
        exit() 
      self.standardInputs['cmdTaskDb'] = options.command_database
    else:
      self.standardInputs['cmdTaskDb'] = 'TaskCMD.db'    
      
    if(options.handler_script):
      self.standardInputs['handlerScript'] = options.handler_script
    else:
      self.standardInputs['handlerScript'] = None 
#
# The main event loop 
#    
  def run(self):
    
    workDirName = os.getcwd()
    
    cmdTaskDb  = self.standardInputs['cmdTaskDb']
    
    #
    # check to see if the requisite directories exist
    #
    self.getTaskList()
    for i in self.taskList:
      self.taskID = str(i)
      self.getRunWorkingDirectory()

    taskTemplateFile = self.getPathFromOSpath("CreateTaskCommandDb.tpl")
    f = open(taskTemplateFile,'rU')
    taskFileContents =  f.read()
    f.close()
    taskTemplate = Template(taskFileContents)
#
#
    xmlParams = {}
    xmlParams['executableCommand']         = self.standardInputs['command'] 
    xmlParams['CreateTaskCommandTemplate'] = self.getPathFromOSpath("CreateTaskCommand.tpl")
    xmlParams['taskTable']                 = self.standardInputs['taskTableName']
    xmlParams['taskDirectory']             = self.standardInputs['taskDir']
    
    if(self.standardInputs['handlerScript'] == None ):
      xmlParams['outputHandlerScript'] = ' '
    else:
      xmlParams['outputHandlerScript']  = "<outputHandlerScript value = \"" \
      + self.standardInputs['handlerScript'] + "\" type = \"file\" />"                           
    
    taskXMLFile = taskTemplate.substitute(xmlParams)
    fName       =   workDirName + os.path.sep + "CreateTaskCommandsDb.xml"
    self.writeToUnixFile(taskXMLFile, fName)
    
    options = {}
    options['xmlTaskFile'] = fName
    options['runDBname']   = cmdTaskDb
    options['task_table']  = self.standardInputs['taskTableName']
    options['force']       = True
    self.taskDbBuilder.setStandardOptions(options)
    self.taskDbBuilder.run()
#
#   Fill the database task for each task in the original task database
#
    sqCon  = sqlite3.connect(cmdTaskDb)
    sqCon.isolation_level = None
    sqDB   = sqCon.cursor()

    for k in range(0,self.taskList.__len__()):
      self.taskDbBuilder.taskData['task_id'] = k+1
      self.taskDbBuilder.runParameters['command'] = self.standardInputs['command']
      self.taskDbBuilder.jobTasks.insertJobTask(sqDB,sqCon,self.taskDbBuilder.taskData,self.taskDbBuilder.runParameters,self.taskDbBuilder.outputData)
#
#   Close down the database
#
    sqDB.close()
    sqCon.close()
    
    print() 
    print('########################################################')
    print('       Database    --> %s                               ' % cmdTaskDb)
    print('       Task Table  --> %s                               ' % self.standardInputs['taskTableName'])
    print('########################################################')
    print() 
    print('Command To Be Executed: ')
    print(self.standardInputs['command']) 
    print('Directories: ')

    for k in range(1,self.taskList.__len__()+1):
      self.taskID = str(k)
      self.getRunWorkingDirectory()
      print("Task " + self.taskID + " "  + self.workDirName) 
#
#   Remove extraneous files
#
    #os.remove(fName)
    
    
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
    createTaskCommandDb = CreateTaskCommandDb()
    createTaskCommandDb.parseOptions()
    createTaskCommandDb.run()

