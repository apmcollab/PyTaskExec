import os
import string
import sys
import optparse
import random
import time
import xml.dom.minidom
import sqlite3
import os.path
import re
from ParseTaskXML import parseTaskXML
#
#############################################################################
#
# Copyright  2021 Chris Anderson
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

 
class CaptureOutput(object):
    """
  A routine that extracts output data task output and packs the database.
  
  Required parameters 
  
        (-d) --database : Job database
        
        (-x) --xmltaskfile  : xml file specifying output data values to be captured
                              in the format of an PyTaskExec input task file.
                              (Typically, it will be the PyTaskExec input task file)
            
  Optional parameters  
  
        (-t) --tasktable : Table containing task data (if not default)
        
        (-o) --output   : Output directory containing for task output 
        
        (-a) --alternateTaskPrefix : Specifies an alternate prefix to the task output file name. 
                                     Default name is Task, resulting in output file names of the form Task_XXX.output. 
                                     If specified as "none" then a task output file is not used during processing.
                                       
    """
#
# The main event loop 
#        
    def run(self):
        try :
            f = open(self.xmlFile,'r')
        except IOError as exception:
            print('                 === Error ===')
            print(" XML file cannot be read") 
            print(exception)
            exit()
      
        taskBuilderInput   = xml.dom.minidom.parse(f)
        outputDataElement  = taskBuilderInput.getElementsByTagName('outputData')[0]

        parseTask = parseTaskXML()
        self.outputData    = parseTask.createDictFromXMLelement(outputDataElement)
        self.outputKeys    = list(self.outputData.keys())
        
        self.outputType = {}
        for i in range(len(self.outputKeys)):
            self.outputType[self.outputKeys[i]] = self.getSQLiteType(self.outputData[self.outputKeys[i]]);
    
        self.jobDBname     = self.getFullPath(self.jobDBname)
        
        self.getTaskCount()
        self.taskKeys   = {}
        self.taskData   = {}
    
        self.localDirectory  = os.getcwd()
        
        for task in range(1,self.taskMax+1):
            self.taskID = '%d' % task
            self.captureTaskData()
            #if(self.taskData['status'] != 'done'):
            #    continue
            self.getRunWorkingDirectory()
            if(self.workDirName == None):
                continue
            os.chdir(self.workDirName)
            
            if(self.prefix_Task_ID != None):
                ExecOutputName = self.prefix_Task_ID + '_' + self.taskIDout  + '.output'
            else:
                ExecOutputName = 'Task_'+ self.taskIDout  + '.output'
            
            if(self.prefix_Task_ID != "none"):
                if(not (os.path.exists(ExecOutputName))):
                     print('Warning: Task Output File '+ ExecOutputName + ' Doesn\'t Exist')
                     os.chdir(self.localDirectory)
                     continue
                f  = open(ExecOutputName, 'r')
                self.runOutput =  f.read()
                self.runOutput = self.runOutput.replace("\r\n","\n")
                self.outputLines = self.runOutput.splitlines();
                print('Processing Task  ' + self.taskID + '         File = ' + ExecOutputName)
            else:
                self.outputLines = {}
                print('Processing Task  ' + self.taskID) 
                   
        # Use capture output data values  
        
            self.getData()

        #
        # Change back to local directory 
        #
            os.chdir(self.localDirectory)
        #
        # pack output 
        #
            self.packOutput()
   
  
    def getData(self):
        #
        # scrape the output for lines of the form dataName : dataValue 
        # and capture dataValue as a string 
        #
        self.outputDataAsString = {}
        for dataName in list(self.outputData.keys()):
            for i in range(len(self.outputLines)):
                comparisonItems = self.outputLines[i].split()
                comparisonLine = '_'.join(comparisonItems)
                comparisonLine =comparisonLine.replace('-',"DS")
                comparisonLine =comparisonLine.replace('<',"LT")
                comparisonLine =comparisonLine.replace('>',"GT")
                comparisonLine =comparisonLine.replace('&',"AMP")
                comparisonLine =comparisonLine.replace('@',"AT")
                comparisonLine =comparisonLine.replace('(','')
                comparisonLine =comparisonLine.replace(')','')
                if(comparisonLine.find(dataName) != -1):
               		s =self.outputLines[i].split(':')
               		stmp = s[0][:]
               		if(any(ele == dataName.strip() for ele in stmp.split())):
               		  s = s[1].split()
               		  self.outputDataAsString[dataName] = s[0].strip();
               		  print(dataName,self.outputDataAsString[dataName])
                    #s = self.outputLines[i].split(':')
                    #if(s[0].strip() == dataName.strip()):
                    #    s = s[1].split()
                    #    self.outputDataAsString[dataName] = s[0].strip();
         
        # pack the data output values into outputData forcing type information
                      
        self.outputData = {}
        for key in list(self.outputDataAsString.keys()):
            dataType = self.outputType[key]
            if((dataType == u'real')or(dataType == u'REAL')):
                self.outputData[key] = float(self.outputDataAsString[key])
            if((dataType == u'integer')or(dataType == u'INTEGER')):
                self.outputData[key] = int(self.outputDataAsString[key])
            if((dataType == u'text')or(dataType == u'TEXT')):
                self.outputData[key] =  self.outputDataAsString[key]
         
                        
    def packOutput(self): 
        sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
        sqDB   = sqCon.cursor()
 
        for key in list(self.outputData.keys()):       
            sqCommand = 'SELECT ' + key + ' from ' + self.runTableName + ' where ROWID = 1;'
            try:
                sqDB.execute(sqCommand)
            except sqlite3.OperationalError as e :
                print("Database field not found " + key + ' : Adding ' +  key + ' ' + self.outputType[key])
                sqCommand = 'ALTER TABLE '  + self.runTableName + ' ADD COLUMN ' +  key + ' ' + self.outputType[key] + ';'
                sqDB.execute(sqCommand)
                
# Pack output data into the database

        sqCommand = 'UPDATE ' + self.runTableName + " SET status = \'done\' "
        for key in list(self.outputData.keys()):
            val         = self.outputData[key]
            valType = self.getSQLiteType(val)
            if(valType != "BLOB"):
                sqCommand = sqCommand +  ', ' + key + '=' + self.insertVal(val,valType)
                
        sqCommand = sqCommand + self.sqIDtailString
 
        # Connect to database and update with output. Induce use of sqLite3's 
        # autocommit mode by setting isolation_level to None

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
            print('                                       === Error ===')
            print(" File not found") 
            print('\'' + fileName + '\'')
            exit() 
            
    def getRunWorkingDirectory(self):
        if (not (os.path.isdir(self.alternateOutputDirectory))): 
            print('Error: Job Output Directory \''+ self.alternateOutputDirectory \
            +'\' Doesn\'t Exist')
            exit()

        # Try indexing suffix 001, 002, 003, ... 999 
        
        self.taskIDout = self.taskID
        if   (int(self.taskID) <= 9) : self.taskIDout = '00' + self.taskID
        elif (int(self.taskID) <= 99) : self.taskIDout = '0'  + self.taskID
        else : self.taskIDout = self.taskID
  
        self.workDirName =  self.alternateOutputDirectory + os.path.sep + self.runTableName \
        + '_' + self.taskIDout
        
        if (os.path.isdir(self.workDirName)) : 
            return
            
        # Try indexing suffix 1,2,3 ...
        
        self.taskIDout = self.taskID
        self.workDirName =  self.alternateOutputDirectory + os.path.sep + self.runTableName \
        + '_' + self.taskIDout    
        
        if (not (os.path.isdir(self.workDirName))) : 
            print('Warning: Task Output Directory for Task '+ self.taskID + ' Doesn\'t Exist')
            self.workDirName = None
        
    def captureTaskData(self):
        #
        # Caputure the task attributes 
        #
        sqCon  = sqlite3.connect(self.jobDBname,isolation_level = None)
        sqDB   = sqCon.cursor()
        self.sqIDtailString = " WHERE ROWID=" + self.taskID + ';'
        
        if(len(self.taskKeys)== 0):
            sqCommand = 'SELECT taskDataNames from ' + self.runTableName + '_support  WHERE ROWID = 1;'
            self.executeDB(sqDB,sqCommand)
            taskDataNames =  sqDB.fetchall()[0]
            self.taskKeys = taskDataNames[0].split(':')

        for i in range(len(self.taskKeys)):
            sqCommand  = 'SELECT ' + self.taskKeys[i] + ' from ' + self.runTableName + self.sqIDtailString
            self.executeDB(sqDB,sqCommand)
            self.taskData[self.taskKeys[i]]=sqDB.fetchone()[0]
                  
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
             

    def insertVal(self,val,valType):
        if(valType == 'TEXT'):        return  "\'" + val + "\'"
        if(valType == 'INTEGER'): return '%d'            % val
        if(valType == 'REAL'):        return '%-.16e'  % val
        if(valType == 'BLOB'):        return val
         
    def getSQLiteType(self,val):
        if(type(val) is int        ): return 'INTEGER'
        if(type(val) is bytes  ): return 'TEXT' 
        if(type(val) is str        ): return 'TEXT' 
        if(type(val) is float  ): return 'REAL'
        return 'BLOB'
   
    def getSQLiteValue(self,val):
        if(type(val) is int        ): return '%d'            % val
        if(type(val) is bytes  ): return  "\'" + val + "\'" 
        if(type(val) is str        ): return  "\'" + val + "\'" 
        if(type(val) is float  ): return '%-.16e'  % val
        return val
  

    def parseOptions(self):
        p = optparse.OptionParser()
        p.add_option('--database','-d',action='store',dest='run_database',\
                                       help='(required) Specifies job task database file')
        
        p.add_option('--xmltaskfile','-x',action='store',dest='xml_taskfile',\
                                        help='(required) XML file containing a specification of the output data to be collected. Output data specified as in the input file used for TaskDbBuilder)')
        
        p.add_option('--output','-o',action='store',dest='output_directory',\
                                       help='(required) Output directory created by ExecRun for task results ')
  
        p.add_option('--tasktable','-t',action='store',dest='task_table',\
                 help='(optional) Specifies task database table name (if not specified, default is used)')
                 
        p.add_option('--alternateTaskPrefix','-a',action='store',type='string',\
                             dest='prefix_Task_ID', help='(optional) Specifies an alternate prefix to the task output file name. Default name is Task, resulting in output file names of the form Task_XXX.output.') 

        
        options,arguments = p.parse_args()
        

        
        if(not(options.run_database)): 
            print('Run database file name must be specified')
            print('Specify using --database filename or -d filename ')
            exit()
        else:
            self.jobDBname = options.run_database
            
        if(not(options.xml_taskfile)):
            print('CaptureOutputs Error:')
            print('XML tasks file must be specified')
            print('use -x or --xmltaskfile options to specify')
            exit()
        else:
            self.xmlFile = options.xml_taskfile
         
        if(options.task_table): 
            self.runTableName  = options.task_table
        else:                  
            self.runTableName   = (os.path.basename(self.jobDBname).split("."))[0]
         
                
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
        captureOutput = CaptureOutput()
        captureOutput.parseOptions()
        captureOutput.run()

