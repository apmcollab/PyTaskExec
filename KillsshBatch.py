from __future__ import print_function
import os
import pwd
import subprocess
import re
import signal
#
# Recursively determine all processes whose parent PID is specified by 
# parentPID and add to the descendentsList, and print out the 
# fact that these processes will be killed. 
#
def getProcessDescendents(parentPID,pidList,descendentsList):
  for i in pidList:
    pidInfo = i.split()
    pid     = pidInfo[1]
    ppid    = pidInfo[2]
    if(ppid == parentPID):
      descendentsList.append(pid)
      print("Killing  " + i) 
      getProcessDescendents(pid,pidList,descendentsList)
  return
  
def execCommand(runCommand):
  p = subprocess.Popen(runCommand,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  (r,e) = (p.stdout, p.stderr)
  runError = e.read();
  runError = runError.replace("\n",'')
  if(runError != ''): 
    print(runError)
    e.close()
  runOutput = r.read()
  r.close()
  return runOutput

class KillsshBatch(object):
  """
  A script that kills all TaskExec's and programs spawned by them
  for Unix systems.
  
  This command assumes that the output of the Unix command
  
  ps -f -u loginName
  
  has the form
  
  logingName  PID PPID ...
  
  The TaskExec's PID's are determined by scanning the ps output for
  a line that contains both "python" and "TaskExec.py". 
      
  """
  def __init__(self):
    self.userName = pwd.getpwuid(os.getuid())[0]

# The main event loop 
#    
  def run(self):
    pymatch = re.compile('python')
    tmatch  = re.compile('TaskExec.py')
#
#   Capture the machine names from the sshTaskConfig file
#
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
    index = -1
    for i in configFileLines:
      name = (i.split(":")[0]).strip()
      if(name != ''):
        nodeNames.append(name)
        index = index+1
        
    for i in range(len(os.sys.path)):
      killProg = os.sys.path[i] + os.path.sep + 'KillUnixTaskExecs.py'
      if(os.path.isfile(killProg)):
         break
    
    for node in nodeNames:  
      runCommand = "ssh " + node + " python  " + killProg
      execCommand(runCommand)
    #
    # End of loop over nodes
    #
#
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
    killsshBatch= KillsshBatch()
    killsshBatch.run()

