from __future__ import print_function
import threading
import subprocess
import Queue
import os

class SSHexecThread(threading.Thread):
    def __init__(self, id, response_queue,command):
        threading.Thread.__init__(self, name="SSHexecThread_%d" % (id,))
        self.response_queue = response_queue
        self.command    = command
    def run(self):
      runCommand = 'ssh  ' + self.command
      print(runCommand)
      p = subprocess.Popen(runCommand,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
      (r,e) = (p.stdout, p.stderr)
      result = r.read()
      r.close()
      e.close() 
      self.response_queue.put(result)