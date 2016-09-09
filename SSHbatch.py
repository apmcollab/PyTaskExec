from __future__ import print_function
import subprocess
import os
import time
from string import Template
from operator import itemgetter
import threading
import datetime
import Queue
import time
import os
from SSHexecThread import SSHexecThread

class SSHbatch:
  def __init__(self,dirName):
    self.threadStartFlag = True
    self.nodeList        = []
        
    os.sys.path.append(os.path.dirname(dirName))
    try:
      configFile = open('sshTaskConfig.txt')
    except IOError as exception:
      print('\n\n             xxx Error xxx ')
      print('       sshTaskConfig.txt not found \n')
      print('Create this file in the current directory and specify \n')
      print('node_name   : process_count \n')
      print('for each node to be used. ')
      exit(1)
    
    configFileLines = (configFile.read()).split("\n")
    configFile.close()
     
    nodeNames = []
    nodeCount = {}
    index = -1
    for i in configFileLines:
      name = (i.split(":")[0]).strip()
      if(name != ''):
        nodeNames.append(name)
        index = index+1
        if(len(i.split(":")) > 1):
          nodeCount[nodeNames[index]] = (i.split(":")[1]).strip()
        else:
          nodeCount[nodeNames[index]] = 1
#
#   Check the load on machine's nodes -- 
#
    loadValues = {}
    nodeSorted = []
    for i in nodeNames:
      errFlag    = 0
      runCommand = 'ssh ' + i + ' cat /proc/loadavg' 
      p = subprocess.Popen(runCommand,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      (r,e) = (p.stdout, p.stderr)
      errorMsg = e.read().replace('\n','')
      if(errorMsg != ''):
        print("\nXXX Error in starting SSH batch XXXX\n")
        print("Command checking load status returned an error\n") 
        print("Offending command : " + runCommand)
        print("Returned message  : " + errorMsg)
        print("")
        exit(1)
      else:
        runOutput = r.read()
        data = runOutput.split()
        avg1, avg5, avg15 = map(float, data[:3])
        loadValues[i] = avg1
        
    loadSorted = sorted(loadValues.items(), key=itemgetter(1))
    for i in loadSorted:
        nodeSorted.append(i[0])
        
    for i in nodeSorted:
      for j in range(0,int(nodeCount[i])):
        self.nodeList.append(i)
        
      
  def createAndSubmitBatchCmdFile(self,execCommandInfo,response_queue):
    execIndex      = execCommandInfo['execIndex']
    execWorkingDir = execCommandInfo['execWorkingDir'] 
    runCommand     = execCommandInfo['runCommand']
    pythonCommand  = execCommandInfo['pythonCommand']  
    
    nodeCount = self.nodeList.__len__()
    print("Starting Exec # ", execIndex)
    nodeIndex         = (execIndex-1)%nodeCount
    
    cmdParameters = {}
    cmdParameters['execIndex']      = execIndex;
    cmdParameters['Exec_N']         = self.nodeList[nodeIndex]  + '_%d' % execIndex
    cmdParameters['Exec_N_out']     = self.nodeList[nodeIndex]  + '_%d.out' % execIndex
    cmdParameters['ExecWorkingDir'] = execWorkingDir 
    cmdParameters['runCommand']     = runCommand + ' -e ' + self.nodeList[nodeIndex]  + '_%d' % execIndex
    cmdParameters['date']           = time.asctime()
    cmdParameters['pythonCommand']  = pythonCommand


    command = self.nodeList[nodeIndex] + ' ' + pythonCommand  + ' ' + \
                 cmdParameters['runCommand'] + ' > ' +  execWorkingDir + os.path.sep \
               + cmdParameters['Exec_N_out']
    SSHexecThread(nodeIndex,response_queue,command).start()
    print("Executing: ", end=' ')
    print('ssh ' + ' ' + command)

