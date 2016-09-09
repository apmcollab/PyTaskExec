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
  

class KillUnixTaskExecs(object):
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
    # Get a snapshot of PID's 
    #
    runCommand = "ps -f -u " + self.userName 
    p = subprocess.Popen(runCommand,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (r,e) = (p.stdout, p.stderr)
    self.runOutput = r.read();
    sqError        = e.read();
    sqError = sqError.replace("\n",'')
    if(sqError != ''): 
        print(sqError)
    r.close()
    e.close()  
    outputLines = self.runOutput.splitlines()
    
    #
    # Scan for any TaskExec's indicated by the combination of a 
    # line and a TaskExec.py 
    # 
    TaskExecPID     = []
    TaskExecProcess = []
    for i in outputLines:
        if((pymatch.search(i) != None) and (tmatch.search(i) != None)):
            TaskExecPID.append(i.split()[1])
            TaskExecProcess.append(i)
            
    if(TaskExecPID.__len__() == 0):
      print("No running TaskExec instances found")
      exit()
    #
    # Stop all TaskExecPID's so they will not spawn any more processes
    #
    for i in range(TaskExecPID.__len__()):
        print("Stopping " + TaskExecProcess[i])
        os.kill(int(TaskExecPID[i]),signal.SIGSTOP)
    #
    # Re-read the PID list 
    #
    runCommand = "ps -f -u " + self.userName 
    p = subprocess.Popen(runCommand,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (r,e) = (p.stdout, p.stderr)
    self.runOutput = r.read();
    sqError        = e.read();
    sqError = sqError.replace("\n",'')
    if(sqError != ''): 
        print(sqError)
    r.close()
    e.close()  
    outputLines = self.runOutput.splitlines()
    #
    # Scan the PID list for all tasks that are descendents of the 
    # TaskExecPID and then kill them 
    #
    for i in range(TaskExecPID.__len__()):
      descendentsList = []
      getProcessDescendents(TaskExecPID[i],outputLines,descendentsList)
      for j in descendentsList:
        try:
          os.kill(int(j),signal.SIGKILL)
        except:
          continue
    #
    # Kill TaskExec's
    #
    for i in range(TaskExecPID.__len__()):
      print("Killing  " + TaskExecProcess[i])
      try:
        os.kill(int(TaskExecPID[i]),signal.SIGKILL)
      except:
        continue
          
#
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
    killUnixTaskExecs= KillUnixTaskExecs()
    killUnixTaskExecs.run()

