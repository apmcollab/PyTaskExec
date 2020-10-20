from __future__ import print_function
import threading
import datetime
try:
    import Queue
except ImportError:
    import queue
import time
import os

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
