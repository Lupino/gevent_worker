"""Inter-process communication helpers."""

import os
import gevent
from gevent import core, monkey
monkey.patch_os()

class WorkerController(object):
    """Master's channel to a worker pipe (and PID)."""

    def __init__(self, pid, pipe_r=None, pipe_w=None):
        self._pid = pid
        if pipe_r and not hasattr(pipe_r, "fileno"):
            pipe_r = os.fdopen(pipe_r, "r")
        if pipe_w and not hasattr(pipe_w, "fileno"):
            pipe_w = os.fdopen(pipe_w, "w")
        self.rfile = pipe_r
        self.wfile = pipe_w
        self.in_buff = []
        self.out_buff = []

    def __str__(self):
        return "Worker PID #%d" % self._pid

    def __repr__(self):
        a = self._pid, self.wfile.fileno(), self.rfile.fileno()
        return "<%s of %s (%d, %d)>" % ((self.__class__.__name__,) + a)

    def __eq__(self, other):
        if hasattr(other, "pid"):
            other = other.pid
        return self._pid == other

    def __cmp__(self, other):
        if hasattr(other, "pid"):
            other = other.pid
        return cmp(self._pid, other)

    def __hash__(self):
        return hash(self._pid)

    @property
    def pid(self):
        return self._pid

    def notify(self, msg):
        self.out_buff.append(msg + "\n")
        data = "".join(self.out_buff)
        wrote = os.write(self.wfile.fileno(), data)
        data = data[wrote:]
        self.out_buff[:] = [data]

    def begin_notify_receive(self, callback):
        gevent.spawn(self._cb_notify_read, callback)

    def _cb_notify_read(self, callback):
        while True:
            chunk = os.read(self.rfile.fileno(), 4096)
            if not chunk:
                gevent.spawn(callback, chunk)
                return
            self.in_buff.append(chunk)
            if "\n" in chunk:
                notifies = "".join(self.in_buff).split("\n")
                self.in_buff[:] = [notifies.pop()]
                for msg in notifies:
                    gevent.spawn(callback, msg)
