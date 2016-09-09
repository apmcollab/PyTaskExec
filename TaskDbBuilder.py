from __future__ import print_function
import os
import subprocess
import time
import datetime
import optparse

import sqlite3
import xml.dom.minidom
from TaskBuilder import TaskBuilder
from TaskBuilder import checkForExistingDatabase
from TaskBuilder import testRunDataFile
from parseTaskXML import parseTaskXML

#
# This routine creates the database for a parametric run
# 

class TaskDbBuilder(object): 
  def __init__(self):
    self.standardInputs = {}
    self.standardInputs['force']               =  False
    self.standardInputs['runDBname']           = 'Task.db'
    self.standardInputs['xmlTaskFile']         =  None
    self.standardInputs['includePaths']         =  None
    self.taskData = {}

  def parseStandardInputs(self):
#
# parse input options to override hard-coded defaults 
#
    p = optparse.OptionParser()
    
    p.add_option('--xmltaskfile','-x',action='store',dest='xml_taskfile',\
                 help='XML file specifying tasks')
    
    p.add_option('--database','-d',action='store',dest='run_database',\
                 help='Specifies job task database file ')
    
    p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
    
    p.add_option('--force','-f',action='store_true',dest='force',\
                 help='Force overwriting of existing database ')
    
    p.add_option('--include','-I',action='store',dest='include_paths',\
                 help="""Additional search paths for required files.   
                         Multiple entries separated by ;""")
        
    options,arguments = p.parse_args()
    
    if(options.xml_taskfile):
      self.standardInputs['xmlTaskFile'] = options.xml_taskfile
    else:
      print('TaskDbBuilder Error:')
      print('XML tasks file must be specified')
      print('use -x or --xmltaskfile options to specify')
      exit()
      
    if(options.run_database): 
         self.standardInputs['runDBname'] = options.run_database   
         
    if(options.task_table): 
     self.standardInputs['task_table'] = options.task_table  
    else:
     self.standardInputs['task_table'] = (os.path.basename(self.standardInputs['runDBname']).split("."))[0]
      
    if(options.force): 
         self.standardInputs['force'] = options.force
         
    if(options.include_paths):
     self.standardInputs['includePaths'] = options.include_paths
     includePaths = options.include_paths.split(';')
     for i in range(len(includePaths)):
         os.sys.path.append(includePaths[i])
         
  def setStandardOptions(self,options):
    if('xmlTaskFile' in options):
      self.standardInputs['xmlTaskFile'] = options['xmlTaskFile']
    else:
      print('TaskDbBuilder Error:')
      print('XML tasks file must be specified')
      print('use -x or --xmltaskfile options to specify')
      exit()
      
    if('runDBname' in options): 
         self.standardInputs['runDBname'] = options['runDBname']
         
    if('task_table' in options): 
     self.standardInputs['task_table'] = options['task_table']
    else:
     self.standardInputs['task_table'] = (os.path.basename(self.standardInputs['runDBname']).split("."))[0]
      
    if('force' in options): 
         self.standardInputs['force'] = options['force']
         
    if('includePaths' in options):
     standardOptions['includePaths'] = options['includePaths']
     includePaths = options['includePaths'].split(';')
     for i in range(len(includePaths)):
         os.sys.path.append(includePaths[i])
    
#
#=========== End of parseStandardInputs ==========================
#   
 
#
#===========     Begin Main Program   ============================
#  
  def run(self):

#   localize inputs

    xmlFile   = self.standardInputs['xmlTaskFile']
    runDBname = self.standardInputs['runDBname'] 
#
#   The default task table has the same name 
#   as the database prefix . This is done so 
#   that python job execution scripts can infer the 
#   task table from the name of the database file.
#
#   runTableName = (os.path.basename(runDBname).split("."))[0]
    runTableName = self.standardInputs['task_table']

    try :
      f = open(xmlFile,'rU')
    except IOError as exception:
      print('                 === Error ===')
      print(" XML file cannot be read") 
      print(exception)
      exit()
      
    taskBuilderInput = xml.dom.minidom.parse(f)

    jobDataElement       = taskBuilderInput.getElementsByTagName('jobData')[0]
    runParametersElement = taskBuilderInput.getElementsByTagName('runParameters')[0]
    outputDataElement    = taskBuilderInput.getElementsByTagName('outputData')[0]

    if(taskBuilderInput.getElementsByTagName('taskRanges').length != 0):
      taskRangesElement    = taskBuilderInput.getElementsByTagName('taskRanges')[0]

    parseTask = parseTaskXML()
    self.runParameters = parseTask.createDictFromXMLelement(runParametersElement)
    self.outputData    = parseTask.createDictFromXMLelement(outputDataElement)
    
    #
    # Extract job data, use an extraction method that identifies files that will be
    # stored as binary objects in the database. 
    #
    self.jobData  = self.extractJobData(jobDataElement)
#
#   Create database instance file and (if desired) remove an
#   existing table with the same name
#
    sqCon  = sqlite3.connect(runDBname)
    sqDB   = sqCon.cursor()
    #sqCon.isolation_level = None
    checkForExistingDatabase(sqDB,sqCon,runDBname,\
                         runTableName,self.standardInputs['force'])
    sqDB.close()
    sqCon.close()
#
# Check template file to make sure that it is constructed properly
#
    runFileTemplate = self.jobData['runFileTemplate_name']
    testRunDataFile(runFileTemplate,self.runParameters)
#
#   Create the task table 
#
    sqCon  = sqlite3.connect(runDBname)
    sqCon.isolation_level = None
    sqDB   = sqCon.cursor()
    self.jobTasks = TaskBuilder(sqDB,sqCon,runTableName,self.taskData,self.jobData,
                       self.runParameters,self.outputData)

    if(taskBuilderInput.getElementsByTagName('taskRanges').length == 0):
      return 0  
      
    print() 
    print('########################################################')
    print('       Database    --> %s                               ' % runDBname)
    print('       Task Table  --> %s                               ' % runTableName)
    print('########################################################')
    print() 

#
#   Create TaskList using the taskRangesElement 
#
    taskCount = []
    taskCount.append(0)
    paramList = {}

    for taskElements in taskRangesElement.childNodes:
      parseTask.generateTasks(taskElements,self.runParameters,taskCount,paramList)
  
#parseTask.taskList now contains the listing of the tasks
     
    taskIndex = 0
    for i in parseTask.taskList:
      for k in self.runParameters.keys(): self.runParameters[k] = i[k]
      taskIndex += 1
      self.taskData['task_id'] = taskIndex
      self.jobTasks.insertJobTask(sqDB,sqCon,self.taskData,self.runParameters,self.outputData)
  #
  #    print the tasks and the parameters that are varying 
  #
      s = '%3d' % taskIndex
      print(s, end=' ')  
      for k in paramList.keys(): print(' ',k,' ',self.runParameters[k], end=' ') 
      print(' ')

#
#   Close down the database
#
    sqDB.close()
    sqCon.close()
    return 0
#
#
#===========     End Main Program   ============================
#  

  def extractJobData(self,dictElement):
    """

    """
    parseTask = parseTaskXML()
    dictArray = {}
    fileNameList = None
    for dataVal in dictElement.childNodes:
      if(dataVal.nodeType is xml.dom.Node.ELEMENT_NODE):
        if(dataVal.getAttribute('type').lower() == 'file'):
          fileData = dataVal.nodeName + "_data"
          fileName = dataVal.nodeName + "_name"
          if(fileNameList == None ) : fileNameList = dataVal.nodeName
          else:                       fileNameList = fileNameList + ':'+ dataVal.nodeName
          dictArray[fileData] = None
          dictArray[fileName] = dataVal.getAttribute('value')
        else:
          dictArray[dataVal.nodeName] = parseTask.getTypedValue(dataVal.getAttribute('value'),\
                                                          dataVal.getAttribute('type'))
#
#   Add input xml task file 
#
    dictArray["xmlTaskFile_name"] = self.standardInputs['xmlTaskFile'];
    dictArray["xmlTaskFile_data"] = None
    if(fileNameList == None ):
      fileNameList = 'xmlTaskFile'
    else:
      fileNameList = fileNameList + ':' + 'xmlTaskFile'
#
#   If an output handler file is specified, then determine the class name from the
#   file name. 
#  
    if('outputHandlerScript_name' in dictArray):
          dictArray['outputHandlerClass_name'] = os.path.basename(dictArray['outputHandlerScript_name']).split('.')[0]
          
    dictArray["fileNames"] = fileNameList
    return dictArray
#
#   Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
  taskDbBuilder = TaskDbBuilder()
  taskDbBuilder.parseStandardInputs()
  taskDbBuilder.run()






















