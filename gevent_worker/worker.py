import os
import sys

from gevent import event
import gevent

from . import signal_stop

class Worker(object):

    def __init__(self):
        self.stop_event = event.Event()
        self.alive = 1

    def _cb_stop_worker(self):
        self.alive = 0
        self.stop_event.set()

    def start(self):
        gevent.signal(signal_stop, self._cb_stop_worker)
        gevent.spawn(self.run)

    def run(self):
        pass


    def handle_notification(self, msg):
        if not msg:
            self.log_server("Master seems to have died, harakiri")
        elif msg == "stop":
            self.log_server("Master says stop")
            self.alive = 0
            self.stop_event.set()
        elif msg == "hello":
            self.log_server("Master says hi")  # DEBUG
        else:
            self.handler(msg)

    @property
    def ident(self):
        return str(os.getpid())

    def log_server(self, msg):
        print >> sys.stderr, "%8s %s\n" % (self.ident, msg)

    def handler(self, msg):
        print(msg)

    def serve_forever(self):
        self.start()
        self.stop_event.wait()
        self.log_server("worker stop")
