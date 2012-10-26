gevent_worker
=============

A worker for gevent 1.0

Usage:

    from gevent_worker.master import Master
    from gevent_worker.worker import Worker

    class MyWorker(Worker):

        def __init__(self):
            Worker.__init__(self)

        def run(self):
            do_some_action

        def handler(self, msg):
            print(msg)
            do_some_action

    master = Master()

    for i in range(0,10):
        master.start_worker(MyWorker)

    master.serve_forver()
