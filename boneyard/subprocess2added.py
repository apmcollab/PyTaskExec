### This module is based on the recipe 440554 of the Python Cookbok online:
### http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/440554
###
### This module is published under the Python license.
### See http://www.python.org/2.4/license for licensing details.

'''Enhanced subprocess module.

Title:          Module to allow Asynchronous subprocess use on Windows and
                Posix platforms
Submitter:      Josiah Carlson (other recipes)
Last Updated:   2006/12/01
Version no:     1.9
Category:       System

Description:

The 'subprocess' module in Python 2.4 has made creating and accessing
subprocess streams in Python relatively convenient for all supported platforms,
but what if you want to interact with the started subprocess? That is, what if
you want to send a command, read the response, and send a new command based on
that response?

Now there is a solution. The included subprocess.Popen subclass adds three new
commonly used methods: recv(maxsize=None), recv_err(maxsize=None), and
send(input), along with a utility method: send_recv(input='', maxsize=None).

recv() and recv_err() both read at most maxsize bytes from the started
subprocess.
send() sends strings to the started subprocess.
send_recv() will send the provided input, and read up to maxsize bytes from
both stdout and stderr.

If any of the pipes are closed, the attributes for those pipes will be set to
None, and the methods will return None.

v. 1.3      fixed a few bugs relating to *nix support
v. 1.4,5    fixed initialization on all platforms, a few bugs relating to
            Windows support, added two utility functions, and added an example
            of how to use this module.
v. 1.6      fixed linux _recv() and test initialization thanks to Yuri
            Takhteyev at Stanford.
v. 1.7      removed _setup() and __init__() and fixed subprocess unittests
            thanks to Antonio Valentino. Added 4th argument 'tr' to
            recv_some(), which is, approximately, the number of times it will
            attempt to recieve data. Added 5th argument 'stderr' to
            recv_some(), where when true, will recieve from stderr.
            Cleaned up some pipe closing.
v. 1.8      Fixed missing self. parameter in non-windows _recv method thanks
            to comment.
v. 1.9      Fixed fcntl calls for closed handles.

-------------------------------------------------------------------------------

Modifications by Antonio Valentino <a_valentino@users.sourceforge.net>:

* stop method added. It allows to kill the sub-process both from Windows and
  Posix.

'''

import os
import time
import errno
import subprocess

if subprocess.mswindows:
    from win32file import ReadFile, WriteFile
    from win32pipe import PeekNamedPipe
    import msvcrt

    from subprocess import STARTUPINFO, STARTF_USESHOWWINDOW, pywintypes
    from win32api import OpenProcess, TerminateProcess, CloseHandle
else:
    import select
    import fcntl
    import signal

from subprocess import list2cmdline
from subprocess import PIPE, STDOUT, call, check_call, CalledProcessError

__all__ = ["Popen", "PIPE", "STDOUT", "call", "check_call", "CalledProcessError"]

class Popen(subprocess.Popen):
    delay_after_stop = 0.2

    def recv(self, maxsize=None):
        return self._recv('stdout', maxsize)

    def recv_err(self, maxsize=None):
        return self._recv('stderr', maxsize)

    def send_recv(self, input='', maxsize=None):
        return self.send(input), self.recv(maxsize), self.recv_err(maxsize)

    def get_conn_maxsize(self, which, maxsize):
        if maxsize is None:
            maxsize = 1024
        elif maxsize < 1:
            maxsize = 1
        return getattr(self, which), maxsize

    def _close(self, which):
        getattr(self, which).close()
        setattr(self, which, None)

    if subprocess.mswindows:
        def send(self, input):
            if not self.stdin:
                return None

            try:
                x = msvcrt.get_osfhandle(self.stdin.fileno())
                (errCode, written) = WriteFile(x, input)
            except ValueError:
                return self._close('stdin')
            except (subprocess.pywintypes.error, Exception), why:
                if why[0] in (109, errno.ESHUTDOWN):
                    return self._close('stdin')
                raise

            return written

        def _recv(self, which, maxsize):
            conn, maxsize = self.get_conn_maxsize(which, maxsize)
            if conn is None:
                return None

            try:
                x = msvcrt.get_osfhandle(conn.fileno())
                (read, nAvail, nMessage) = PeekNamedPipe(x, 0)
                if maxsize < nAvail:
                    nAvail = maxsize
                if nAvail > 0:
                    (errCode, read) = ReadFile(x, nAvail, None)
            except ValueError:
                return self._close(which)
            except (subprocess.pywintypes.error, Exception), why:
                if why[0] in (109, errno.ESHUTDOWN):
                    return self._close(which)
                raise

            if self.universal_newlines:
                read = self._translate_newlines(read)
            return read

        def stop(self, force=True):
            if self.poll() is not None:
                return True

            try:
                PROCESS_TERMINATE = 1
                handle = OpenProcess(PROCESS_TERMINATE, False, self.pid)
                TerminateProcess(handle, -1)
                CloseHandle(handle)
            except pywintypes.error, e:
                # @TODO: check error code
                warnings.warn(e)

            time.sleep(self.delay_after_stop)
            if self.poll() is not None:
                return True
            else:
                return False

    else:
        def send(self, input):
            if not self.stdin:
                return None

            if not select.select([], [self.stdin], [], 0)[1]:
                return 0

            try:
                written = os.write(self.stdin.fileno(), input)
            except OSError, why:
                if why[0] == errno.EPIPE: #broken pipe
                    return self._close('stdin')
                raise

            return written

        def _recv(self, which, maxsize):
            conn, maxsize = self.get_conn_maxsize(which, maxsize)
            if conn is None:
                return None

            flags = fcntl.fcntl(conn, fcntl.F_GETFL)
            if not conn.closed:
                fcntl.fcntl(conn, fcntl.F_SETFL, flags| os.O_NONBLOCK)

            try:
                if not select.select([conn], [], [], 0)[0]:
                    return ''

                r = conn.read(maxsize)
                if not r:
                    return self._close(which)

                if self.universal_newlines:
                    r = self._translate_newlines(r)
                return r
            finally:
                if not conn.closed:
                    fcntl.fcntl(conn, fcntl.F_SETFL, flags)

        def _kill(self, sigid):
            '''Ignore the exception when the process doesn't exist.'''
            try:
                os.kill(self.pid, sigid)
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise

        def stop(self, force=True):
            '''This forces a child process to terminate.

            It starts nicely with SIGTERM.
            If "force" is True then moves onto SIGKILL.
            This returns True if the child was terminated.
            This returns False if the child could not be terminated.
            '''

            # @TODO: SIGINT, SIGHUP
            if self.poll() is not None:
                return True
            self._kill(signal.SIGTERM)
            time.sleep(self.delay_after_stop)
            if self.poll() is not None:
                return True
            if force:
                self._kill(signal.SIGKILL)
                time.sleep(self.delay_after_stop)
                if self.poll() is not None:
                    return True
                else:
                    return False
            return False
