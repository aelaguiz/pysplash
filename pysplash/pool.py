# -*- coding: utf-8 -*-

import time
import os
import signal
import random
import redis
import rq
import psutil
from Queue import Empty

from .log import logger
from .worker import Worker
from .accounting import PoolAccounting

from multiprocessing import Process, Queue

log = logger()


def msg_exit(wname):
    return {'msg': 'exit', 'pid': os.getpid(), 'wname': wname}


def msg_update(wname):
    return {'msg': 'update', 'pid': os.getpid(), 'wname': wname}


def msg_failed(wname):
    return {'msg': 'failed', 'pid': os.getpid(), 'wname': wname}


def msg_started(wname):
    return {'msg': 'started', 'pid': os.getpid(), 'wname': wname}



def _worker(wname, pool_queue, args):
    log.debug("Worker %s started" % wname)

    try:
        pool_queue.put(msg_started(wname))


        def exc_handler(job, *args):
            log.error("Job %s Excepted" % (job.id))

        def work_callback(job):
            log.debug("Worker %s completed job %s %s" % (
                wname, job.id, job.status))

            if job.status == rq.job.Status.FAILED:
                pool_queue.put(msg_failed(wname))
            else:
                pool_queue.put(msg_update(wname))

        def status_callback():
            """
            Controls execution of worker. Worker will exit when queue
            is empty if this callback returns False
            """
            if args['retire_idle']:
                return False

            return True

        con = redis.StrictRedis(
            host=args['host'], port=args['port'], password=args['password'],
            db=args['db'])

        queues = [rq.Queue(q, connection=con) for q in args['queues']]

        rqw = Worker(
            queues,
            status_callback=status_callback,
            exc_handler=exc_handler,
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

        self.acct = PoolAccounting()

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

        # Number of seconds to wait while gathering initial stats on workers
        # before scaling up
        self.quiet_period_seconds = kwargs.setdefault(
            'quiet_period_seconds', 10.0)

        # Maximum cpu utilization
        self.max_cpu = kwargs.setdefault('max_cpu', 80.0)

        # Maximum mem utilization
        self.max_mem = kwargs.setdefault('max_mem', 80.0)

        self.args = kwargs

        # Should workers without a job to do be spun down immediately?
        self.args['retire_idle'] = kwargs.setdefault('retire_idle', True)

        self.args['queues'] = queues
        self.args['host'] = host
        self.args['port'] = port
        self.args['password'] = password
        self.args['db'] = db

        self.args['main_pid'] = os.getpid()

        # Workers we've scaled down and are waiting on exiting
        self.waiting_scale_down = []

        self.stats = []
        self.pool_queue = Queue()

        self.con = redis.StrictRedis(
            host=self.args['host'], port=self.args['port'], password=self.args['password'],
            db=self.args['db'])
        self.rqs = [rq.Queue(q, connection=self.con) for q in self.args['queues']]



    def start(self):
        self.start_time = time.time()

        self.establish_baseline()
        self.acct.start()

        while True:
            num_running, num_starting = self.update_workers()

            self.acct.set_workers(num_running)

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
            self.acct.log()

    def establish_baseline(self):
        cpu = []
        mem = []

        log.debug("Establishing a baseline reading for cpu & memory...")
        for i in range(5):
            cpu_pct = psutil.cpu_percent()
            mem_pct = psutil.virtual_memory().percent

            cpu.append(cpu_pct)
            mem.append(mem_pct)

            time.sleep(1)

        self.baseline_cpu = sum(cpu) / float(len(cpu))
        self.baseline_mem = sum(mem) / float(len(mem))

        log.debug("Baseline cpu & memory reading: %s CPU %s Memory" % (
            self.baseline_cpu, self.baseline_mem))

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
                    self.on_worker_exit(wname)
                elif obj['msg'] == 'started':
                    worker['state'] = WorkerState.RUNNING
                    log.debug("Worker %s became ready" % worker['w'].name)
                elif obj['msg'] == 'failed':
                    log.debug("Worker %s reported failure" % worker['w'].name)
                    worker['last_update'] = time.time()
                    self.acct.add_failed()

            except Empty:
                break

    def count_outstanding_queue(self):
        cnt = 0

        for queue in self.rqs:
            cnt += queue.count

        return cnt

    def on_worker_exit(self, wname):
        self.acct.add_exited()

        worker = self.workers[wname]
        if worker in self.waiting_scale_down:
            self.waiting_scale_down.remove(worker)

        del self.workers[wname]

    def update_stats(self, num_running, num_starting):
        cpu_pct = psutil.cpu_percent()
        mem_pct = psutil.virtual_memory().percent

        log.debug("CPU %s Memory %s" % (cpu_pct, mem_pct))

        self.stats.append((num_running, num_starting, cpu_pct, mem_pct))

        if len(self.stats) > 30:
            self.stats.pop(0)

    def scale_pool(self, total_workers):
        # Collect enough stats to matter
        if (time.time() - self.start_time) < self.quiet_period_seconds:
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
                cpu_per_running += ((cpu_pct - self.baseline_cpu) / float(num_running))
                mem_per_running += ((mem_pct - self.baseline_mem) / float(num_running))


        avg_cpu_per_running = max(float(cpu_per_running) / len(self.stats), 0.01)
        avg_mem_per_running = max(float(mem_per_running) / len(self.stats), 0.01)

        cpu_pct = psutil.cpu_percent()
        mem_pct = psutil.virtual_memory().percent

        log.debug("CPU %s Mem %s AvgC/R %s AvgM/R %s" % (
            cpu_pct, mem_pct, avg_cpu_per_running, avg_mem_per_running))

        cpu_workers = (self.max_cpu - cpu_pct) / avg_cpu_per_running
        mem_workers = (self.max_mem - mem_pct) / avg_mem_per_running

        avail_workers = int(min(
            self.max_procs - total_workers, min(cpu_workers, mem_workers)))

        log.debug("%s CPU Bound Worker %s Mem Bound Worker %s Potential Workers" % (
            cpu_workers, mem_workers, avail_workers))

        delta = min(avail_workers, self.max_per_scale)

        self.scale_pool_delta(total_workers, delta)

    def scale_pool_delta(self, total_workers, delta):
        if delta > 0:
            log.debug("Starting %s workers" % delta)

            for i in range(delta):
                self.add_worker()
        elif delta < 0:
            log.debug("Should scale down the number of workers")
            if self.waiting_scale_down:
                log.debug("Already waiting on %s to scale down" % (
                    len(self.waiting_scale_down)))
                return

            to_kill = min(abs(delta), self.max_procs)

            # Make sure we leave the desired min procs running
            if to_kill >= (total_workers - self.min_procs):
                # 8 Running 6 To kill 2 Min = 0 = 6
                # 8 Running 8 to kill 2 min = -2 = 6
                to_kill += (total_workers - to_kill) - self.min_procs

            if to_kill <= 0:
                log.debug(
                    "Cannot kill any more, would leave us below min procs")
                return

            for wname in random.sample(self.workers.keys(), to_kill):
                worker = self.workers[wname]
                self.waiting_scale_down.append(worker)
                self.terminate_worker(worker)


    def update_workers(self):
        num_running = 0
        num_starting = 0

        waiting_terminate = 0

        for wname, worker in self.workers.items():
            state = worker['state']
            since_update = time.time() - worker['last_update']

            if state == WorkerState.TERMINATED:
                if (time.time() - worker['terminate_time']) > self.terminate_seconds:
                    log.warning(
                        "Worker %s didn't terminate, sending SIGKILL" % (
                            wname))
                    self.really_terminate_worker(worker)
                else:
                    waiting_terminate += 1
            elif state == WorkerState.RUNNING:
                if since_update > self.zombie_timeout:
                    log.info("Worker %s zombied" % (worker['w'].name))
                    self.terminate_worker(worker)
                    self.acct.add_zombie()
                else:
                    num_running += 1
            elif state == WorkerState.STARTING:
                num_starting += 1

        self.acct.set_waiting_terminate(waiting_terminate)

        return num_running, num_starting

    def terminate_worker(self, worker):
        worker['state'] = WorkerState.TERMINATED
        worker['terminate_time'] = time.time()
        worker['w'].terminate()

    def really_terminate_worker(self, worker):
        try:
            os.kill(worker['w'].pid, signal.SIGKILL)
            os.kill(worker['w'].pid, signal.SIGKILL)
        except:
            pass
        finally:
            self.on_worker_exit(worker['w'].name)

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
