import os
import sys
import time
import signal

import gevent

from . import signal_stop
from .ipc import WorkerController

class Master(object):
    exiting = False

    exit_stopped = 0x10

    def __init__(self):
        self.num_workers = 0
        self.workers = []

        self.exit_completed = gevent.event.Event()
        self.stop_event = gevent.event.Event()

    def serve_forever(self):
        self.start()
        self.log_server("Master of disaster, PID " + str(os.getpid()))
        self.stop_event.wait()
        self.log_server("Stop event")

    def start(self):
        """Set up signal actions."""
        gevent.signal(signal.SIGTERM, self._cb_sigterm)

    def start_workers(self, num_workers, worker):
        for i in xrange(num_workers):
            self.start_worker(worker)

    def start_worker(self, worker):
        """Start a worker and add to list."""
        if self.stop_event.is_set():
            return
        self.num_workers += 1
        pipe_r, pipe_w = os.pipe()
        pid = gevent.fork()
        if pid:
            os.close(pipe_r)
            cntr = WorkerController(pid, pipe_w=pipe_w)
            self.workers.append(cntr)
            self.exit_completed.clear()
            cntr.notify("hello")
            return pid
        os.close(pipe_w)
        worker = worker()
        cntr = WorkerController(os.getpid(), pipe_r=pipe_r)
        cntr.begin_notify_receive(worker.handle_notification)
        try:
            worker.serve_forever()
        except KeyboardInterrupt:
            pass
        os._exit(self.exit_stopped)

    def _cb_sigterm(self):
        """Initiate stopping sequence"""
        self.log_server("SIGTERM, initiating stop")
        gevent.spawn(self.stop)

    def stop(self, timeout=1):
        """Issue a stop event, and wait for the stop to complete."""
        self.log_server("Stopping")
        self.stop_event.set()
        self.exiting = True
        gevent.spawn(self.notify_workers, "stop")
        gevent.spawn_later(1, self.kill_workers)
        self.exit_completed.wait(timeout=timeout)
        if not self.exit_completed.is_set():
            wpdesc = ", ".join(map(str, self.workers))
            self.log_server("Forcefully killing workers: " + wpdesc)
            self.kill_workers(signal.SIGTERM)

    def notify_workers(self, *args):
        """Send a notification to all workers."""
        for worker in self.workers:
            worker.notify(*args)

    def notify_worker(self, idx, *args):
        """Send a notification to worker."""
        self.workers[idx].notify(*args)

    def kill_workers(self, signum=signal_stop):
        """Send a signal to all workers."""
        for worker in self.workers:
            os.kill(worker.pid, signum)

    @property
    def ident(self):
        if self.num_workers > 1:
            return "%d-master" % os.getpid()
        else:
            return "master"

    def log_server(self, msg):
        print >> sys.stderr, "%8s %s" % (self.ident, msg)
