from __future__ import print_function
import sqlite3
import os
import string
import sys
import optparse
import random
import time

 
class UpdateOutputs(object):
  """
  A routine that extracts output data task output and packs the database.
  
  Required parameters 
    (-d) --database : Job database
    
    (-x) --handler  : Python program that parses task output and extracts
                      the required output data
      
  Optional parameters  
  
    (-t) --tasktable : Table containing task data (if not default)
   
    (-I) --include  : Additional search paths for including modules 
                      with multiple entries separated by ;
    
    (-o) --output   : Output directory containing for task output 
    
    (-a) --alternateTaskPrefix : Specifies an alternate prefix to the task output file name. 
                                 Default name is Task, resulting in output file names of the form Task_XXX.output. 
                                 If specifed as "none" then a task output file is not used during processing.
                 
  """
#
# The main event loop 
#    
  def run(self):
    self.outputHandlerName = self.getFullPath(self.outputHandlerName)
    self.jobDBname         = self.getFullPath(self.jobDBname)
    
    self.getTaskCount()
    
    self.importRequiredModules()
    
    self.importOutputHandler()
    
    self.jobKeys    = {}
    self.taskKeys   = {}
    self.outputKeys = {}
    self.paramKeys  = {}
    self.outputData = {}
    self.jobData    = {}
    self.taskData   = {}
    self.runData    = {}
    self.outputDataType = {}

    self.localDirectory  = os.getcwd()
    
    for task in range(1,self.taskMax+1):
      self.taskID = '%d' % task
      self.captureTaskData()
      if(self.taskData['status'] != 'done'):
        continue
      self.getRunWorkingDirectory()
      os.chdir(self.workDirName)
      

      if(self.prefix_Task_ID != None):
        ExecOutputName = self.prefix_Task_ID + '_' + self.taskIDout  + '.output'
      else:
        ExecOutputName = 'Task_'+ self.taskIDout  + '.output'
      
      if(self.prefix_Task_ID != "none"):
        f              = open(ExecOutputName, 'rU')
        self.runOutput =  f.read()
        runOutput      = self.runOutput.replace("\r\n","\n")
        outputLines = self.runOutput.splitlines();
        print('Processing Task  ' + self.taskID + '     File = ' + ExecOutputName)
      else:
       outputLines = {}
       print('Processing Task  ' + self.taskID) 
        
      handlerArgs = {}
      handlerArgs['outputLines']   = outputLines
      handlerArgs['jobData']       = self.jobData
      handlerArgs['runData']       = self.runData
      handlerArgs['taskData']      = self.taskData
      handlerArgs['outputData']    = self.outputData
      handlerArgs['jobDBname']     = self.jobDBname
      handlerArgs['runTableName']  = self.runTableName
      handlerArgs['taskID']        = self.taskID
      handlerArgs['execDirectory'] = self.localDirectory
      handlerArgs['totalTaskCount']= self.taskMax
      handlerArgs['outputDataType']= self.outputDataType

      self.oHandler    = self.outputHandler(handlerArgs);
    #
    # Use the handler class to fill the output data 
    #
      self.oHandler.fillOutputData(outputLines,self.taskData,self.jobData,self.runData,self.outputData)
      
    #
    # Change back to local directory 
    #
      os.chdir(self.localDirectory)
    #
    # pack output 
    #
      self.packOutput()
   
    # call the finalizer after all are processed 
    
    self.oHandler.finalize(handlerArgs)
  
  def packOutput(self): 
  #
  # Pack output data into the database
  #
    sqCommand = 'UPDATE ' + self.runTableName + " SET status = \'done\' "
    for i in range(len(self.outputKeys)):
      val     = self.outputData[self.outputKeys[i]]
      valType = self.getSQLiteType(val)
      if(valType != "BLOB"):
       sqCommand = sqCommand +  ', ' + self.outputKeys[i] + '=' + self.insertVal(val,valType)

    sqCommand = sqCommand + self.sqIDtailString

    #
    # Connect to database and update with output. Induce use of sqLite3's 
    # autocommit mode by setting isolation_level to None
    #
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
    self.executeDB(sqDB,sqCommand)
    sqDB.close()
    sqCon.close()
    #time.sleep(.1)
    
    
  def getTableName(self):
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
    sqCommand = 'select name from SQLite_Master;'
    #sqDB.execute(sqCommand)
    self.executeDB(sqDB,sqCommand)
    self.runTableName = sqDB.fetchone()
    self.runTableName = self.runTableName[0]
    sqDB.close()
    sqCon.close()
    
  def getTaskCount(self):
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
    sqCommand = 'select max(rowid) from ' + self.runTableName;
    #sqDB.execute(sqCommand)
    self.executeDB(sqDB,sqCommand)
    self.taskMax = sqDB.fetchone()
    self.taskMax = self.taskMax[0]
    sqDB.close()
    sqCon.close()
    
  def getFullPath(self, fileName):
    if(os.path.isfile(fileName)):
      return os.path.abspath(fileName)
    else:
      print('                 === Error ===')
      print(" File not found") 
      print('\'' + fileName + '\'')
      exit() 
      
  def getRunWorkingDirectory(self):
    if (not (os.path.isdir(self.alternateOutputDirectory))): 
      print('Error: Job Output Directory \''+ self.alternateOutputDirectory \
      +'\' Doesn\'t Exist')
      exit()

    self.taskIDout = self.taskID
    if   (int(self.taskID) <= 9) : self.taskIDout = '00' + self.taskID
    elif (int(self.taskID) <= 99) : self.taskIDout = '0'  + self.taskID
    else                         : self.taskIDout = self.taskID
  
    self.workDirName =  self.alternateOutputDirectory + os.path.sep + self.runTableName \
    + '_' + self.taskIDout
    if (not (os.path.isdir(self.workDirName))) : 
      print('Error: Job Output Directory \''+ self.workDirName \
      +'\' Doesn\'t Exist')
      exit()
    
  def captureTaskData(self):
    #
    # Caputure the task attributes 
    #
    sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
    sqDB   = sqCon.cursor()
    self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
    
    if(len(self.jobKeys)== 0):
      sqCommand = 'SELECT jobDataNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      jobDataNames =  sqDB.fetchall()[0]
      self.jobKeys = jobDataNames[0].split(':')

    for i in range(len(self.jobKeys)):
      sqCommand  = 'SELECT ' + self.jobKeys[i] + ' from ' + self.runTableName + '_support  WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      self.jobData[self.jobKeys[i]]=sqDB.fetchone()[0]
      
    if(len(self.taskKeys)== 0):
      sqCommand = 'SELECT taskDataNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      taskDataNames =  sqDB.fetchall()[0]
      self.taskKeys = taskDataNames[0].split(':')

    for i in range(len(self.taskKeys)):
      sqCommand  = 'SELECT ' + self.taskKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
      self.executeDB(sqDB,sqCommand)
      self.taskData[self.taskKeys[i]]=sqDB.fetchone()[0]
        
    if(len(self.outputKeys) == 0):
      sqCommand              = 'SELECT outputDataNames from ' + self.runTableName + '_support WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      outputDataNames   = sqDB.fetchall()[0]
      self.outputKeys        = outputDataNames[0].split(':');
        
    for i in range(len(self.outputKeys)):
      sqCommand  = 'SELECT ' + self.outputKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
      self.executeDB(sqDB,sqCommand)
      self.outputData[self.outputKeys[i]] = sqDB.fetchone()[0]
  
    if(len(self.paramKeys)== 0):
      sqCommand              = 'SELECT runParameterNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      runParameterNames = sqDB.fetchall()[0]
      self.paramKeys         = runParameterNames[0].split(':');

    for i in range(len(self.paramKeys)):
      sqCommand  = 'SELECT ' + self.paramKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
      self.executeDB(sqDB,sqCommand)
      self.runData[self.paramKeys[i]] = sqDB.fetchone()[0]

    for i in range(len(self.outputKeys)):
      sqCommand  = 'SELECT typeof(' + self.outputKeys[i] + ') from ' + self.runTableName + ' WHERE ROWID = 1;'
      self.executeDB(sqDB,sqCommand)
      self.outputDataType[self.outputKeys[i]] = sqDB.fetchone()[0]
   
    sqDB.close()
    sqCon.close()
  
  def executeDB(self,sqDB,sqCommand):
   tryCount = 0
   tryMax   = 10
   while(tryCount < tryMax):
     try :
       sqDB.execute(sqCommand)
       tryCount = tryMax
     except sqlite3.OperationalError as e:
       tryCount = tryCount +1
       print(e)
       print("Database locked. Retrying ...")
       sleepTime = .01*random.random()
       time.sleep(sleepTime)
       
  def importOutputHandler(self):
    os.sys.path.append(os.path.dirname(self.outputHandlerName))
    outputClassName = os.path.basename(self.outputHandlerName).split('.')[0]
    outputClassName  = outputClassName + '.' + outputClassName
    try:
      self.outputHandler = self._get_class(outputClassName)
    except ImportError as exception:
      print('Failed to load class ' + outputClassName)
      print(exception.message)
      exit()
    except AttributeError as exception:
      print('Failed to load class ' + outputClassName)
      print(exception.message)
      exit()
      
  def importRequiredModules(self):
    try:
      from classfetch import _get_func,_get_class    #class loader 
    except ImportError as exception:
      print('Failed to load required modules sqLite and classfetch ') 
      print(exception.message)
      exit()
      
  # initialize class instances
    self._get_class       = _get_class   

  def insertVal(self,val,valType):
    if(valType == 'TEXT'):    return  "\'" + val + "\'"
    if(valType == 'INTEGER'): return '%d'      % val
    if(valType == 'REAL'):    return '%-.16e'  % val
    if(valType == 'BLOB'):    return val
     
  def getSQLiteType(self,val):
    if(type(val) is int    ): return 'INTEGER'
    if(type(val) is str    ): return 'TEXT' 
    if(type(val) is unicode): return 'TEXT' 
    if(type(val) is float  ): return 'REAL'
    return 'BLOB'
   
  def getSQLiteValue(self,val):
    if(type(val) is int    ): return '%d'      % val
    if(type(val) is str    ): return  "\'" + val + "\'" 
    if(type(val) is unicode): return  "\'" + val + "\'" 
    if(type(val) is float  ): return '%-.16e'  % val
    return val
  

  def parseOptions(self):
    p = optparse.OptionParser()
    p.add_option('--database','-d',action='store',dest='run_database',\
                 help='Specifies job task database file (required)')
    
    p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
      
    p.add_option('--handler ','-x',action='store',dest='output_handler',\
                 help='Output handler')
        
    p.add_option('--include','-I',action='store',dest='include_paths',\
                 help="""Additional search paths for including modules 
                         with multiple entries separated by ;""")
    
    p.add_option('--output','-o',action='store',dest='output_directory',\
                 help='Output directory for task results')
  
    p.add_option('--alternateTaskPrefix','-a',action='store',type='string',\
             dest='prefix_Task_ID', help='Specifies an alternate prefix to the task output file name. Default name is Task, resulting in output file names of the form Task_XXX.output. If specifed as "none" then a task output file is not used during processing.') 

    
    options,arguments = p.parse_args()
    
    if(not(options.run_database)): 
      print('Run database file name must be specified')
      print('Specify using --database filename or -d filename ')
      exit()
    else:
     self.jobDBname = options.run_database
     
    if(options.task_table): 
         self.runTableName  = options.task_table
    else:        
        self.runTableName   = (os.path.basename(self.jobDBname).split("."))[0]
     
    if(not(options.output_handler)): 
      print('Output handler  must be specified')
      print('Specify using --handler filename or -x filename ')
      exit()
    else:
     self.outputHandlerName  = options.output_handler
     
    if(options.include_paths):
      includePaths = options.include_paths.split(';')
      for i in range(len(includePaths)):
       os.sys.path.append(includePaths[i])
       os.sys.path.append('.')
       
    if(options.output_directory):
      self.alternateOutputDirectory = options.output_directory
    else:
         self.alternateOutputDirectory = '.'   
         
    if(options.prefix_Task_ID):
       self.prefix_Task_ID = options.prefix_Task_ID
    else:
       self.prefix_Task_ID = None 
#
# Stub for invoking the main() routine in this file 
#   
#
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
    updateOutputs = UpdateOutputs()
    updateOutputs.parseOptions()
    updateOutputs.run()

