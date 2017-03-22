from __future__ import print_function
import os
import glob
 
#
# Copy this file to the directory containing the database and TaskData directory
# Set self.extension to the extension or file type that one wants deleted 
# Execute the command 
#
# python -m UpdateOutputs -d [The Data Base].db -x RemoveFilesHandler.py -o TaskData
#
#
class RemoveFilesHandler:
  def __init__(self,handlerArgs):
    self.extension = None 
        
  class handlerError(Exception):
    def __init__(self, message=None):
        self.message = message
    def __repr__(self):
        return repr(self.message)
    
  def fillOutputData(self,outputLines,taskData,jobData,parameterData,outputData):
    print(os.getcwd()) 
    if(self.extension == None) : return
    for fileToRemove in glob.glob(self.extension):
        os.remove(fileToRemove)

  def finalize(self,handlerArgs):
      return
         


  


       
