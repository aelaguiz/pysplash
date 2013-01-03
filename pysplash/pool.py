# -*- coding: utf-8 -*-

import time
import os
import signal
import redis
import rq
from Queue import Empty

from .log import logger
from .worker import Worker

from multiprocessing import Process, Queue

log = logger()


def msg_exit(wname):
    return {'msg': 'exit', 'pid': os.getpid(), 'wname': wname}


def msg_update(wname):
    return {'msg': 'update', 'pid': os.getpid(), 'wname': wname}


def msg_started(wname):
    return {'msg': 'started', 'pid': os.getpid(), 'wname': wname}



def _worker(wname, pool_queue, args):
    log.debug("Worker %s started" % wname)

    try:
        pool_queue.put(msg_started(wname))

        def exc_handler(job, *args):
            pass

        def work_callback(job):
            log.debug("Worker %s completed job %s" % (
                wname, job.id))

            pool_queue.put(msg_update(wname))

        con = redis.StrictRedis(
            host=args['host'], port=args['port'], password=args['password'],
            db=args['db'])

        queues = [rq.Queue(q) for q in args['queues']]

        rqw = Worker(
            queues, name="RQW:" + wname, exc_handler=exc_handler,
            work_callback=work_callback,
            connection=con)
        rqw.log = log

        rqw.work()

    finally:
        pool_queue.put(msg_exit(wname))
        log.debug("Worker %s exited" % wname)


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)
    return type(name, (), values)


WorkerState = enum('WorkerState', STARTING='starting', RUNNING='running', TERMINATED='terminated')


class Pool:
    def __init__(
        self, queues, host='localhost', port=6379, db=None,
            password=None, zombie_timeout=400, **kwargs):

        self.count = 0
        self.workers = {}
        self.min_procs = kwargs.setdefault('min_procs', 1)

        self.args = kwargs
        self.args['queues'] = queues
        self.args['host'] = host
        self.args['port'] = port
        self.args['password'] = password
        self.args['db'] = db

        self.args['main_pid'] = os.getpid()
        self.zombie_timeout = zombie_timeout

        self.pool_queue = Queue()

        pass
        #return cls(host=url.hostname, port=url.port, db=db,
                   #password=url.password, **kwargs)

    def start(self):
        while True:
            num_running = self.update_workers()

            log.debug("Pool has %s active workers" % (num_running))

            if num_running < self.min_procs:
                self.add_worker()

            time.sleep(1)
            self.process_queue()

    def process_queue(self):
        while True:
            try:
                obj = self.pool_queue.get_nowait()

                wname = obj['wname']

                if wname not in self.workers:
                    log.warning("Received message %s from unknown %s" % (
                        obj, wname))
                    continue

                worker = self.workers[wname]

                if obj['msg'] == 'update':
                    worker['last_update'] = time.time()
                elif obj['msg'] == 'exit':
                    log.debug("Worker %s exited" % worker['w'].name)
                    worker['state'] = WorkerState.TERMINATED
                elif obj['msg'] == 'started':
                    worker['state'] = WorkerState.RUNNING
                    log.debug("Worker %s became ready" % worker['w'].name)

            except Empty:
                break


    def update_workers(self):
        num_running = 0

        for wname, worker in self.workers.iteritems():
            state = worker['state']
            since_update = time.time() - worker['last_update']

            if state == WorkerState.TERMINATED:
                pass
            elif state == WorkerState.RUNNING:
                if since_update > self.zombie_timeout:
                    log.info("Worker %s zombied" % (worker['w'].name))
                    self.terminate_worker(worker)
                else:
                    num_running += 1
            elif state == WorkerState.STARTING:
                num_running += 1

        return num_running

    def terminate_worker(self, worker):
        worker['state'] = WorkerState.TERMINATED
        worker['terminate_time'] = time.time()
        worker['w'].terminate()

    def add_worker(self):
        wname = "PySplash-%s" % self.count
        w = Process(target=_worker, args=(wname, self.pool_queue, self.args))
        w.name = wname
        
        self.count += 1

        worker = {}
        worker['state'] = WorkerState.STARTING
        worker['last_update'] = time.time()
        worker['w'] = w

        self.workers[w.name] = worker

        w.start()
