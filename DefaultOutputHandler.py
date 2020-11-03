#
# This is a default output handler 
#
# To use this handler insert
#
# <outputHandlerScript  value = "DefaultOutputHandler.py" />
# 
# into the <jobData> list.
#
# Conventions:
#
# The executing program must output the results in the form
#
# outputDataName : outputDataValue
#
# where outputDataName is a output data name specified in the <outputData> parameter list.
# Extraneous characters before outputDataName are allowed as long as colons aren't present.
#
# Additionally if the outputDataName has spaces in it, say "Time To Completion (minutes)" one replaces any
# contiguous spaces with a single _, parenthesis are deleted, and the symbols <,>,&,@,- are replaced by
# "LT", "GT", "AMP", "AT" and "DS" respectively. Thus, the name specifed would be 
#  Time_To_Completion_minutes
# 

#
# The output data type is specified as one of int, double or string. 
#
# If data that should be collected isn't then it is likely that the data associated with the
# specified output data name isn't being properly output.
#
# The finalize routine is a no-op. 
# 
# Chris Anderson (C) UCLA 2012
# Modifications: Added exact matching for data name 4/8/2014
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
class DefaultOutputHandler:
  def __init__(self,handlerArgs):
    outputLines         = handlerArgs['outputLines']
    self.outputDataType = handlerArgs['outputDataType']
    
    self.outputDataAsString = {}
    for dataName in self.outputDataType.keys():
        for i in range(len(outputLines)):
            comparisonItems = outputLines[i].split()
            comparisonLine = '_'.join(comparisonItems)
            comparisonLine =comparisonLine.replace('-',"DS")
            comparisonLine =comparisonLine.replace('<',"LT")
            comparisonLine =comparisonLine.replace('>',"GT")
            comparisonLine =comparisonLine.replace('&',"AMP")
            comparisonLine =comparisonLine.replace('@',"AT")
            comparisonLine =comparisonLine.replace('(','')
            comparisonLine =comparisonLine.replace(')','')
            if(comparisonLine.find(dataName) != -1):
               s = outputLines[i].split(':')
               if(s[0].strip() == dataName.strip()):
                 s = s[1].split()
                 self.outputDataAsString[dataName] = s[0].strip();    
        
  class handlerError(Exception):
    def __init__(self, message=None):
        self.message = message
    def __repr__(self):
        return repr(self.message)
   
  def checkInt(self,s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
       
  def fillOutputData(self,outputLines,taskData,jobData,parameterData,outputData):
    for i in outputData.keys():
        self.packData(outputData,i) 

  def finalize(self,handlerArgs):
      return
         
  def printData(self,outputData):
    for i in outputData.keys():
        if(i in self.outputDataAsString):
            print(self.outputDataAsString[i])

  def packData(self,outputData,key):
      dataType  = self.outputDataType[key]
      if(key in self.outputDataAsString):
        if((dataType == u'real')or(dataType == u'REAL')):
            outputData[key] = float(self.outputDataAsString[key])
            return
        if((dataType == u'integer')or(dataType == u'INTEGER')):
            outputData[key] = int(self.outputDataAsString[key])
            return
        if((dataType == u'text')or(dataType == u'TEXT')):
            outputData[key] =  self.outputDataAsString[key] 
            return
        if((dataType == u'bool')or(dataType == u'BOOL')):
          if(self.checkInt(self.outputDataAsString[key])):
            outputData[key] = int(self.outputDataAsString[key])
            return
          outputData[key] =  self.outputDataAsString[key] 
          return
            
#NULL.    The value is a NULL value.
#INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
#REAL.    The value is a floating point value, stored as an 8-byte IEEE floating point number.
#TEXT.    The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
#BOOL     The value is treated as a text string with value true or false
#BLOB.    The value is a blob of data, stored exactly as it was input.
  
          

       
