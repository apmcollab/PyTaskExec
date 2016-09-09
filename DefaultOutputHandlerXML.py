from __future__ import print_function
import xml.dom.minidom
from parseTaskXML import parseTaskXML
# Chris Anderson (C) UCLA 2015
#
# A beta version of a default handler for output data that is 
# output in XML format. 
#
# In order for this handler to know the output file containing
# the XML output, the program generating the data must output 
# to the console the name of the data file using syntax
#
# Output_Data_File : [Name of File].xml
#
# 
# The output data in the XML file must be 
# encapuslated in a child tag with name 
#
#        outputData 
#
# Typically, the XML data in outputData is identical
# to that of the <outputData> child in the XML 
# parameter sweep file. 
#
# The parent tag is ignored.
#
# Sample output XML data file:
#
#<?xml version="1.0" ?>
#<!-- XML_ParameterListArray -->
#<Poisson3D_Output_Data>
#    <outputData>
#        <Err_Inf value="3.788578193002282e-03" type="double" />
#        <Err_L2 value="1.354998356094185e-03" type="double" />
#        <SolnNorm_L2 value="2.736546327401750e-01" type="double" />
#        <Total_Time value="3.087000000000000e+00" type="double" />
#    </outputData>
#</Poisson3D_Output_Data>
#
#
# Due to the problems with unicode being replaced by "str" and "bytes" in 
# Python3, this handler only runs under python 2.7
#
# "Python 3 renamed the unicode type to str, the old str type has been replaced by bytes."
#
# August 20, 2015
#
# 

class DefaultOutputHandlerXML:
  def __init__(self,handlerArgs):
    outputLines         = handlerArgs['outputLines']
    self.outputDataType = handlerArgs['outputDataType']
    self.outputXMLFile  = None
    self.outputXMLdata = None

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
      if(comparisonLine.find('Output_Data_File') != -1):
      	s = outputLines[i].split(':')
      	if(s[0].strip() == 'Output_Data_File'.strip()):
            s = s[1].split()
            self.outputXMLFile = s[0].strip()
            print(self.outputXMLFile)
            try :
              f = open(self.outputXMLFile,'rU')
            except IOError as exception:
              print('                 === Error ===')
              print(" XML file cannot be read") 
              print(exception)
              exit()
            xmlData = xml.dom.minidom.parse(f)
            runOutputData = xmlData.getElementsByTagName('outputData')[0]
            parseTask  = parseTaskXML()
            self.outputXMLdata = parseTask.createDictFromXMLelement(runOutputData)   
        
  class handlerError(Exception):
    def __init__(self, message=None):
        self.message = message
    def __repr__(self):
        return repr(self.message)
    
  def fillOutputData(self,outputLines,taskData,jobData,parameterData,outputData):
    for i in outputData.keys():
        self.packData(outputData,i) 

  def finalize(self,handlerArgs):
      return
         
  def printData(self):
    for i in outputData.keys():
        if(i in self.outputDataAsString):
            print(self.outputDataAsString[i])

  def packData(self,outputData,key):
    if(not (key in self.outputXMLdata )):
     return
    dataType  = self.outputDataType[key]
    if((dataType == u'real')or(dataType == u'REAL')):
      outputData[key] = float(self.outputXMLdata[key])
      return
    if((dataType == u'integer')or(dataType == u'INTEGER')):
      outputData[key] = int(self.outputXMLdata[key])
      return
    if((dataType == u'text')or(dataType == u'TEXT')):
       outputData[key] =  self.outputXMLdata[key] 
       return
  
#NULL.    The value is a NULL value.
#INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
#REAL.    The value is a floating point value, stored as an 8-byte IEEE floating point number.
#TEXT.    The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
#BLOB.    The value is a blob of data, stored exactly as it was input.
  
          

       
