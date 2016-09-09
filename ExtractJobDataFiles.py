from __future__ import print_function
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
import StringIO

import sqlite3

class ExtractJobDataFiles(object): 
 """
 Executes tasks specified by an job task database.
 """ 
 def __init__(self):
    self.jobDBname                = None
    self.runTableName             = None
    self.alternateOutputDirectory = None
#
#===========     Begin Main Program   ============================
#  
 def run(self):
   
  sqCon  = sqlite3.connect(self.jobDBname,isolation_level=None)
  sqDB   = sqCon.cursor()
  
  sqCommand = "SELECT fileNames FROM " + self.runTableName + "_support where rowid = 1; "
  self.executeDB(sqDB,sqCommand)
  fileNamesTmp =  sqDB.fetchall()[0][0].split(":")
  
  fileNames = []
  for i in fileNamesTmp :
    if(i != 'outputHandlerClass') : fileNames.append(i)

  print("::: Files to be extracted :::")
  print(" ")
  dataFileNames = {}
  for i in fileNames:
    fileNameName = i + '_name'
    sqCommand    = "SELECT " + fileNameName + '  FROM '  + self.runTableName + "_support where rowid = 1; "
    self.executeDB(sqDB,sqCommand)
    dataFileName = os.path.basename(sqDB.fetchall()[0][0])
    if(self.alternateOutputDirectory == None) :
      dataFileNames[i] = dataFileName
    else:
      dataFileName = self.alternateOutputDirectory + os.path.sep + dataFileName
      dataFileNames[i] = dataFileName
    print(dataFileName)

  print(" ") 
  overWriteFiles = None
  for i in dataFileNames:
    if(os.path.isfile(dataFileNames[i])):
     if(overWriteFiles == None):
       print("One or more data file exists. Overwrite the existing files?")
       print('y)es or n)o : ', end=' ')
       yesNo = (raw_input()).lower()
       if(yesNo[0] != 'y'): 
         print('Program terminated ')
         sqDB.close()
         sqCon.close()
         exit()
       else:
         overWriteFiles = True
  
  print(" ") 
  if(self.alternateOutputDirectory != None):
    if (not (os.path.isdir(self.alternateOutputDirectory))): os.mkdir(self.alternateOutputDirectory)
    
  for i in fileNames:
    dataName     = i + '_data'
    sqCommand = "SELECT " + dataName + '  FROM '  + self.runTableName + "_support where rowid = 1; "
    self.executeDB(sqDB,sqCommand)
    dataFile     = sqDB.fetchall()[0][0]
    print("writing " + dataFileNames[i]) 
    fileHandle   = StringIO.StringIO(buffer(dataFile))
    dataFileTmp  = fileHandle.read() 
    fileHandle.close() 
    self.writeToUnixFile(dataFileTmp,dataFileNames[i])

  sqDB.close()
  sqCon.close()  

 #############################################################
 #    End of while loop in run()  
 #############################################################


 def parseStandardOptions(self):
     p = optparse.OptionParser()
     p.add_option('--database','-d',action='store',dest='run_database',\
                 help='Specifies job task database file (required)')
     
     p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='Specifies task database table name')
     
     p.add_option('--output','-o',action='store',dest='output_directory',\
                 help='Output directory for task results')
     
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

     if(options.output_directory):
         self.alternateOutputDirectory = options.output_directory
     else:
         self.alternateOutputDirectory =  None
        
 def setOptions(options):
   if(not('run_database' in options)): 
     print('Run database file name must be specified')
     print('Specify using --database filename or -d filename ')
     exit()
   else:
     self.jobDBname = options['run_database']
         
   if('task_table' in options): 
     self.runTableName = options['task_table']
   else:        
     self.runTableName = (os.path.basename(self.jobDBname).split("."))[0]
       
   if('output_directory' in options):
     self.alternateOutputDirectory = options['output_directory']
   else:
     self.alternateOutputDirectory = '.' 
     
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
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
  extractJobDataFiles = ExtractJobDataFiles()
  extractJobDataFiles.parseStandardOptions()
  extractJobDataFiles.run()