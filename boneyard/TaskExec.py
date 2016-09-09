import os
import shutil
import string
import subprocess
import time
import string
from   string import Template
import optparse

def main(): 
    taskExec = TaskExec()
    taskExec.run()
    
class TaskExec(object): 
 """
 Executes tasks specified by an job task database.
 
 This class requires that a program output handler 
 class be available 
 
 """ 
 def __init__(self):
  self.maxTryCount  = 100  # Maximal number of attempts to read db before exiting
#
# The main event loop 
#    
 def run(self):
     
  self.silentRun = False
  self.exec_id   = os.getcwd() # default ID = current directory
  
  self.parseOptions()
  
  self.importRequiredModules()

  localDirectory    = os.getcwd()

  self.sqDB             = self.sqLite(self.jobDBname)
  self.sqDB.maxTryCount = self.maxTryCount
  self.sqDB.verbose     = True
  self.runTableName     = (os.path.basename(self.jobDBname).split("."))[0]
  
  self.sqDB.createConnection()

  self.outputKeys = {}
  self.paramKeys  = {}
  self.jobKeys    = {}
  self.jobData    = {}
  self.runData    = {}
  self.outputData = {}
  
  taskCount          = 0
  self.taskAvailable = True
  self.taskID       = '0'

  while((self.taskAvailable)and((taskCount < self.maxTaskCount) or (self.maxTaskCount == 0))):
      
      self.getTaskID();
      if(not self.taskAvailable): break
   
      # Create a working directory for the run, and write the data file to it
   
      self.createRunWorkingDirectory()
      
      #
      # Create the run data file using the input template and run data 
      #
      self.createRunDataFile()

      #
      # Execute the command in the working directory
      #

      print 'Running code'
      executableName  = self.jobData['executableName']
      os.chdir(self.workDirName)
      runCommand = executableName + ' ' + self.runFileName
      print runCommand
    
      p = subprocess.Popen(runCommand,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      (r,e) = (p.stdout, p.stderr)
      self.runOutput = r.read();
      sqError        = e.read();
      if(os.path.sep == '\\') : sqError = sqError.replace("\r\n",'')
      else:                     sqError = sqError.replace("\n",'')
      if(sqError != ''): print sqError
      r.close()
      e.close()        
      
      print 'Done'
      #
      # Change back to local directory 
      #
      os.chdir(localDirectory)
      
      if(not self.silentRun): 
         printOutput = self.runOutput.replace("\r\n","\n")
         print printOutput

      #  process the output and capture output data 
    
      self.handleProgramOutput()

      #  pack the output into the database
    
      self.packOutput()
    
      taskCount = taskCount + 1
 #############################################################
 #    End of while loop in run()  
 #############################################################
  self.sqDB.closeConnection()
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
 
 def parseOptions(self):
     p = optparse.OptionParser()
     p.add_option('--database','-d',action='store',dest='run_database',\
                 help='Specifies job task database file (required)')
     p.add_option('--include','-I',action='store',dest='include_paths',\
                 help="""Additional search paths for including modules 
                         with multiple entries separated by ;""")
     p.add_option('--output','-o',action='store',dest='output_directory',\
                 help='Output directory for task results')
     p.add_option('--ntaskCount','-n',action='store',type='int',default=0,\
             dest='max_task_count', help='Maximum number of tasks to perform')
     p.add_option('--silent','-s',action='store_true',\
             dest='silent', help='Suppress output of programs run by TaskExec')
     p.add_option('--execID','-e',action='store',type='string',\
             dest='exec_id', help='Specify ID for TaskExec')
     options,arguments = p.parse_args()
    
     if(not(options.run_database)): 
         print 'Run database file name must be specified'
         print 'Specify using --database filename or -d filename '
         exit()
     else:
         self.jobDBname = options.run_database

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
         
     if(options.silent):
         self.silentRun = options.silent
         
     if(options.exec_id):
         self.exec_id = options.exec_id
         
#
# Import required modules. This routine must be called after the 
# parsing of the input data, as additional path are set based upon
# users input
#
 def importRequiredModules(self):
   try:
      from sqLite     import  sqLite                 #interface to SQlite db
      from sqLite     import sqLiteError
      from pOpen2    import  recv_someTimeout
      from classfetch import _get_func,_get_class    #class loader 
   except ImportError, exception:
      print 'Failed to load required modules sqLite and classfetch ' 
      print exception.message
      exit()
      
   # initialize class instances
   
   self.sqLite           = sqLite
   self._get_class       = _get_class
   self.sqLiteError      = sqLiteError
   self.recv_someTimeout = recv_someTimeout
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
 # Lock on the database
 #
  try:
    self.sqDB.getLock()
  except self.sqLiteError, e:
    print e.message
    self.sqDB.closeConnection()
    exit()
    
 # The following code is sensitive to the response of the system
 # and if things don't work properly, try increasing the 
 # sqLite.sqTime paramter. This is the time that the subprocess 
 # waits for a response.
 #
 # Fetch undone tasks, and select the top one
 # (later I can put in a random selection)
 #
  sqCommand      = 'SELECT ROWID from ' + self.runTableName + " WHERE status=\'task\';"
  try:
    taskList = self.sqDB.execute(sqCommand)
  except self.recv_someTimeout:
    taskList = ''
  except self.sqLiteError, e:
    print  self.exec_id + '  ' + e.message
    self.sqDB.clearLock()
    self.sqDB.closeConnection()
    exit()

  if os.sys.platform == 'win32':
        taskList    = taskList.split("\r\n")
  else: 
        taskList    = taskList.split("\n")
  
  #print self.exec_id + ' TaskList ' + `taskList`
  
  # Check for no tasks left, skip the task and return
 
  if(taskList[0] == ''): 
    self.taskAvailable = False
    print 'All tasks complete '
    self.sqDB.clearLock()
    return 

  if os.sys.platform == 'win32':
       self.taskID = taskList[0].replace("\r\n",'')
  else:
       self.taskID = taskList[0].replace("\r",'')
       
  print self.exec_id + ' got task ' + self.taskID
  #
  # Update the task status in the job data base to 'exec' and specify
  # the local directory as the exec_id
  #
  self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
  sqCommand    = 'UPDATE ' + self.runTableName \
                      + " SET status = \'exec\', exec_id = " \
                      + "\'" + self.exec_id  + "\'" +  self.sqIDtailString
  try:
       self.sqDB.executeNoReturn(sqCommand)
       self.sqDB.clearLock()
  except self.sqLiteError, e : 
       print e.messasge
  #
  # Caputure the task attributes and data
  #
  self.captureTaskData(self.runTableName,self.taskID,self.sqDB)
  

 
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
     
 def importTemplateFileAndOutputHandler(self):
  #
  # Caputure the template file name, open it, and read the contents
  # Obtain info about it from the first task 
  #
  runTemplateFile = self.jobData['templateFileName']
  f = open(runTemplateFile,'rU')
  self.runFileContents =  f.read()
  f.close()
  
  self.runTemplate = Template(self.runFileContents)
  
 #
 # Instantiate the output handler to handle the output 
 #
  outputHandlerName = self.jobData['outputHandlerName']
  os.sys.path.append(os.path.dirname(outputHandlerName))
  outputHandlerName = os.path.basename(outputHandlerName).split('.')[0]
  outputHandlerName  = outputHandlerName + '.' + outputHandlerName
  try:
       self.outputHandler = self._get_class(outputHandlerName)
  except ImportError, exception:
        print 'Failed to load class ' + outputHandlerName
        print exception.message
        exit()
  except AttributeError, exception:
        print 'Failed to load class ' + outputHandlerName
        print exception.message
        exit()
     
 def createRunDataFile(self):
  #
  # Substitute in the parameters
  #
  self.runDataFile =  self.runTemplate.substitute(self.runData)
  self.runFileName =  self.runTableName + '_' + self.taskID + '.qdt'
  fName       =  self.workDirName + os.path.sep + self.runFileName
  
  self.writeToUnixFile(self.runDataFile, fName)
  
 def createRunWorkingDirectory(self):
  if (not (os.path.isdir(self.alternateOutputDirectory))): os.mkdir(self.alternateOutputDirectory)
  #
  # Write out the data file to the run directory 
  #
  if   (int(self.taskID) <= 9) : taskIDout = '00' + self.taskID
  elif (int(self.taskID) < 99) : taskIDout = '0'  + self.taskID
  else                         : taskIDout = self.taskID
  
  self.workDirName =  self.alternateOutputDirectory + os.path.sep + self.runTableName + '_' + taskIDout
  if (not (os.path.isdir(self.workDirName))) : os.mkdir(self.workDirName)

    
 def handleProgramOutput(self):  
    outputLines = self.runOutput.splitlines();
    oHandler    = self.outputHandler(outputLines,self.jobData,self.runData,self.outputData);
    #
    # Use the handler class to fill the output data 
    #
    oHandler.fillOutputData(self.jobData,self.runData,self.outputData)


 def packOutput(self): 
  #
  # Pack output data into the database
  #
    sqCommand = 'UPDATE ' + self.runTableName + " SET status = \'done\' "
    for i in range(len(self.outputKeys)):
      val     = self.outputData[self.outputKeys[i]]
      valType = self.sqLite.getSQLiteType(val)
      sqCommand = sqCommand +  ', ' + self.outputKeys[i] + '=' + self.sqLite.insertVal(val,valType)

    sqCommand = sqCommand + self.sqIDtailString
    
    try:
     self.sqDB.executeWithRepeatNoReturn(sqCommand)
    except self.sqLiteError, e:
      print e.message
      self.sqDB.closeConnection()
      exit()
    
#
# captures jobKeys, jobData, outputKeys,outputData, paramKeys, runData
#
 def captureTaskData(self,runTableName,taskID,sqDB):
  #
  # Caputure the task attributes (only if non-zero)
  # This routine only uses standard execution, as the data based is
  # locked by the invoking routine (run(..))
  #
  self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
  if(len(self.jobKeys)== 0):
    sqCommand              = 'SELECT jobDataNames from ' + runTableName + self.sqIDtailString
    if os.sys.platform == 'win32':
      jobDataNames      = sqDB.executeWithRepeat(sqCommand).replace("\r\n",'')
    else:
      jobDataNames      = sqDB.executeWithRepeat(sqCommand).replace("\n",'')
    self.jobKeys           = jobDataNames.split(':');
    
    for i in range(len(self.jobKeys)):
      sqCommand  = 'SELECT ' + self.jobKeys[i] + ' from ' + runTableName + self.sqIDtailString
      if os.sys.platform == 'win32':
        self.jobData[self.jobKeys[i]]=sqDB.executeWithRepeat(sqCommand).replace("\r\n",'')
      else:
        self.jobData[self.jobKeys[i]]=sqDB.executeWithRepeat(sqCommand).replace("\n",'')
   #
   # On initial pass capture the input template file and output handler 
   #
    self.importTemplateFileAndOutputHandler()
        
  if(len(self.outputKeys) == 0):
    sqCommand              = 'SELECT outputDataAttributes from ' + runTableName + self.sqIDtailString
    if os.sys.platform == 'win32':
      outputDataAttributes   = sqDB.executeWithRepeat(sqCommand).replace("\r\n",'')
    else:
      outputDataAttributes   = sqDB.executeWithRepeat(sqCommand).replace("\n",'')
    self.outputKeys        = outputDataAttributes.split(':');
        
    for i in range(len(self.outputKeys)):
      sqCommand  = 'SELECT ' + self.outputKeys[i] + ' from ' + runTableName + self.sqIDtailString
      if os.sys.platform == 'win32':
        self.outputData[self.outputKeys[i]]=sqDB.executeWithRepeat(sqCommand).replace("\r\n",'')
      else:
        self.outputData[self.outputKeys[i]]=sqDB.executeWithRepeat(sqCommand).replace("\n",'')
    
  if(len(self.paramKeys)== 0):
    sqCommand              = 'SELECT runParameterAttributes from ' + runTableName + self.sqIDtailString
    if os.sys.platform == 'win32': 
      runParameterAttributes = sqDB.executeWithRepeat(sqCommand).replace("\r\n",'')
    else:
      runParameterAttributes = sqDB.executeWithRepeat(sqCommand).replace("\n",'')
      
    self.paramKeys         = runParameterAttributes.split(':');

  for i in range(len(self.paramKeys)):
    sqCommand  = 'SELECT ' + self.paramKeys[i] + ' from ' + runTableName + self.sqIDtailString
    if os.sys.platform == 'win32': 
      self.runData[self.paramKeys[i]]=sqDB.executeWithRepeat(sqCommand).replace("\r\n",'')
    else:
      self.runData[self.paramKeys[i]]=sqDB.executeWithRepeat(sqCommand).replace("\n",'')
#
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
    main()


                 





