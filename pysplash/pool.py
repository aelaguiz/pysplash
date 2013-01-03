# -*- coding: utf-8 -*-

import time
import os
import signal
import redis
import rq
import psutil
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

        # Lower limit on workers
        self.min_procs = kwargs.setdefault('min_procs', 1)

        # Upper limit on workers
        self.max_procs = kwargs.setdefault('max_procs', 128)

        # Maximum number of workers to start in a single round of scaling
        self.max_per_scale = kwargs.setdefault('max_per_scale', 2)

        # Seconds between updates before a worker is considered a zombie
        self.zombie_timeout = zombie_timeout

        # Minimum wait between spawns of new workers
        self.scale_frequency = kwargs.setdefault('scale_frequency', 10.0)

        # Maximum number of seconds waiting before we send a kill -9
        self.terminate_seconds = kwargs.setdefault('terminate_seconds', 10.0)

        self.args = kwargs
        self.args['queues'] = queues
        self.args['host'] = host
        self.args['port'] = port
        self.args['password'] = password
        self.args['db'] = db

        self.args['main_pid'] = os.getpid()


        self.stats = []
        self.pool_queue = Queue()

        self.con = redis.StrictRedis(
            host=self.args['host'], port=self.args['port'], password=self.args['password'],
            db=self.args['db'])
        self.rqs = [rq.Queue(q) for q in self.args['queues']]


    def start(self):
        while True:
            num_running, num_starting = self.update_workers()
            self.update_stats(num_running, num_starting)

            total = num_running + num_starting

            log.debug("Pool has %s active workers (%s starting)" % (
                num_running, num_starting))

            if num_running < self.min_procs:
                self.add_worker()
            else:
                self.scale_pool(total)

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
                    del self.workers[wname]
                elif obj['msg'] == 'started':
                    worker['state'] = WorkerState.RUNNING
                    log.debug("Worker %s became ready" % worker['w'].name)

            except Empty:
                break

    def count_outstanding_queue(self):
        cnt = 0

        for queue in self.rqs:
            cnt += queue.count

        return cnt

    def update_stats(self, num_running, num_starting):
        cpu_pct = psutil.cpu_percent()
        mem_pct = psutil.virtual_memory().percent

        log.debug("CPU %s Memory %s" % (cpu_pct, mem_pct))

        self.stats.append((num_running, num_starting, cpu_pct, mem_pct))

        if len(self.stats) > 30:
            self.stats.pop(0)

    def scale_pool(self, total_workers):
        # Collect enough stats to matter
        if len(self.stats) < 10:
            return

        # Don't scale too frequently
        if time.time() - self.last_scale < self.scale_frequency:
            return

        outstanding = self.count_outstanding_queue()

        log.debug("Outstanding queue length %s" % outstanding)
        if not outstanding:
            return

        cpu_per_running = 0
        mem_per_running = 0

        for num_running, num_starting, cpu_pct, mem_pct in self.stats:
            if num_running:
                cpu_per_running += (cpu_pct / float(num_running))
                mem_per_running += (mem_pct / float(num_running))


        avg_cpu_per_running = float(cpu_per_running) / len(self.stats)
        avg_mem_per_running = float(mem_per_running) / len(self.stats)

        cpu_pct = psutil.cpu_percent()
        mem_pct = psutil.virtual_memory().percent

        cpu_workers = 80. - cpu_pct / avg_cpu_per_running
        mem_workers = 80. - mem_pct / avg_mem_per_running

        avail_workers = int(min(
            self.max_procs - total_workers, min(cpu_workers, mem_workers)))

        log.debug("%s CPU/Worker %s Mem/Worker %s Potential Workers" % (
            cpu_workers, mem_workers, avail_workers))

        to_start = min(avail_workers, self.max_per_scale)

        log.debug("Starting %s workers" % to_start)

        for i in range(to_start):
            self.add_worker()


    def update_workers(self):
        num_running = 0
        num_starting = 0

        for wname, worker in self.workers.iteritems():
            state = worker['state']
            since_update = time.time() - worker['last_update']

            if state == WorkerState.TERMINATED:
                if (time.time() - worker['terminate_time']) > self.terminate_seconds:
                    log.warning(
                        "Worker %s didn't terminate, sending SIGKILL" % (
                            wname))
                    self.really_terminate_worker(worker)
            elif state == WorkerState.RUNNING:
                if since_update > self.zombie_timeout:
                    log.info("Worker %s zombied" % (worker['w'].name))
                    self.terminate_worker(worker)
                else:
                    num_running += 1
            elif state == WorkerState.STARTING:
                num_starting += 1

        return num_running, num_starting

    def terminate_worker(self, worker):
        worker['state'] = WorkerState.TERMINATED
        worker['terminate_time'] = time.time()
        worker['w'].terminate()

    def really_terminate_worker(self, worker):
        os.kill(worker['w'].pid, signal.SIGKILL)

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

        self.last_scale = time.time()
        w.start()
