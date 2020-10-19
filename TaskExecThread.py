from __future__ import print_function
import threading
import datetime
try:
    import Queue
except ImportError:
    import queue
import time
import os

def log(message):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    print("%s %s" % (now, message))
    
class TaskExecThread(threading.Thread):
    def __init__(self, id, response_queue,pythonCommand,runCommand):
        threading.Thread.__init__(self, name="TaskExecThread_%d" % (id,))
        self.response_queue = response_queue
        self.pythonCommand  = pythonCommand
        self.runCommand     = runCommand
    def run(self):
            result = os.spawnv(os.P_WAIT,self.pythonCommand,self.runCommand)
            #log("%s %s " % (self.getName(), result))
            self.response_queue.put(result)