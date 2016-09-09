from __future__ import print_function
import sqlite3
import os
import optparse
import random

class CheckTaskDbStatus(object):
  """
  A routine that checks the status of a task database
  
  Required parameters 
  
    (-d) --database  : Task database
    
    (-t) --tasktable : Table containing task data (if not default)
             
  """
  def __init__(self):
    self.standardInputs = {}
    self.standardInputs['taskDBname']        =  None
    self.standardInputs['taskTableName']     =  None
    self.tasksRemaining = 0
    self.taskCount      = 0
    self.totalTaskCount = 0
    self.doneCount      = 0
    self.execCount      = 0
    self.database       = None
    self.databaseTable  = None

  def parseOptions(self):
    p = optparse.OptionParser()
    p.add_option('--database','-d',action='store',dest='task_database',\
                 help='Specifies job task database file (required)')
    
    p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
      
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

  def setStandardOptions(self,options):
    if('database' in options):
      self.standardInputs['taskDBname'] = options['database']
    else:
      print('Task database file name must be specified')
      exit()
      
    if('tasktable' in options): 
     self.standardInputs['taskTableName'] = options['tasktable']
    else:
     self.standardInputs['taskTableName']  = (os.path.basename(self.standardInputs['taskDBname']).split("."))[0]
      
#
#=========== End of parseStandardInputs ==========================
#   
    
  def run(self):
    taskDBname = self.standardInputs['taskDBname']
    if(not os.path.isfile(taskDBname)):
      return "none"
    
    tableName = self.standardInputs['taskTableName'];
  #
  # check to see if the table exists
  #
    sqCon  = sqlite3.connect(taskDBname,isolation_level=None)
    sqDB   = sqCon.cursor()
    sqCommand = 'SELECT ROWID from ' + tableName + ' where ROWID = 1;'
    try:
     sqDB.execute(sqCommand)
    except sqlite3.OperationalError as e :
      sqDB.close()
      sqCon.close()
      return "none"
    
    sqDB.close()
    sqCon.close()
    
  #
  #   Fill the database task for each task in the original task database
  #
    sqCon  = sqlite3.connect(taskDBname,isolation_level=None)
    sqDB   = sqCon.cursor()
    sqCommand = 'SELECT status from ' + tableName + ";"
    self.executeDB(sqDB,sqCommand)
    taskListData = sqDB.fetchall()
    sqDB.close()
    sqCon.close()
  #
    taskList  = []
    self.totalTaskCount = taskListData.__len__()
    self.taskCount = 0;
    self.doneCount = 0;
    self.execCount = 0;
    for i in range(0,self.totalTaskCount):
      if(taskListData[i][0] == 'done'):
        self.doneCount = self.doneCount + 1
      if(taskListData[i][0] == 'exec'):
        self.execCount = self.execCount + 1
      if(taskListData[i][0] == 'task'):
        self.taskCount = self.taskCount + 1
  
    if(self.doneCount == self.totalTaskCount):
      return "done"
    else:
      self.tasksRemaining = self.totalTaskCount - self.doneCount
      return "running"   
#
  def getRemainingTaskCount(self):
    return self.tasksRemaining
  def getExecutingTaskCount(self):
    return self.execCount
  def getTotalTaskCount(self):
    return self.totalTaskCount
  def getCompletedTaskCount(self):
    return self.doneCount
  def getDatabaseName(self):
    return self.standardInputs['taskDBname']
  def getTableName(self):
    return self.standardInputs['taskTableName'];
    
    tableName = self.standardInputs['taskTableName'];
  
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
  checkTaskDbStatus = CheckTaskDbStatus()
  checkTaskDbStatus.parseOptions()
  print("Database   : " + checkTaskDbStatus.getDatabaseName())
  print("Task Table : " + checkTaskDbStatus.getTableName())
  print("Status     : " + checkTaskDbStatus.run())
  print("Total Task Count     : " , checkTaskDbStatus.getTotalTaskCount())
  print("Executing Task Count : " , checkTaskDbStatus.getExecutingTaskCount())
  print("Completed Task Count : " , checkTaskDbStatus.getCompletedTaskCount())
  print("Remaining Task Count : " , checkTaskDbStatus.getRemainingTaskCount())
