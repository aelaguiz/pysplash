# -*- coding: utf-8 -*-

import rq
import time

from rq.worker import green, yellow, blue, StopRequested
from rq.exceptions import NoQueueError, UnpickleError
from rq.queue import Queue


class Worker(rq.Worker):
    def __init__(
        self, queues, status_callback, work_callback=None, name=None, default_result_ttl=500,
            connection=None, exc_handler=None):  # noqa

        self.status_callback = status_callback
        self.work_callback = work_callback

        super(Worker, self).__init__(
            queues, name, default_result_ttl, connection,
            exc_handler)

    def fork_and_perform_job(self, job):
        super(Worker, self).fork_and_perform_job(job)

        self.work_callback(job)

    def work(self, burst=False):  # noqa
        """Starts the work loop.

        Pops and performs all jobs on the current list of queues.  When all
        queues are empty, block and wait for new jobs to arrive on any of the
        queues, unless `burst` mode is enabled.

        The return value indicates whether any jobs were processed.
        """
        self._install_signal_handlers()

        did_perform_work = False
        self.register_birth()
        self.log.info('RQ worker started, version %s' % rq.version.VERSION)
        self.state = 'starting'
        try:
            while True:
                if self.stopped:
                    self.log.info('Stopping on request.')
                    break
                self.state = 'idle'
                qnames = self.queue_names()
                self.procline('Listening on %s' % ','.join(qnames))
                self.log.info('')
                self.log.info('*** Listening on %s...' %
                              green(', '.join(qnames)))
                wait_for_job = not burst
                try:
                    result = Queue.dequeue_any(self.queues, False,
                                               connection=self.connection)
                    while not result and wait_for_job and self.status_callback():
                        time.sleep(0.1)
                        result = Queue.dequeue_any(self.queues, False,
                                                   connection=self.connection)
                    if result is None:
                        break
                except StopRequested:
                    break
                except UnpickleError as e:
                    msg = '*** Ignoring unpickleable data on %s.' % \
                        green(e.queue.name)
                    self.log.warning(msg)
                    self.log.debug('Data follows:')
                    self.log.debug(e.raw_data)
                    self.log.debug('End of unreadable data.')
                    self.failed_queue.push_job_id(e.job_id)
                    continue

                self.state = 'busy'

                job, queue = result
                self.log.info('%s: %s (%s)' % (green(queue.name),
                                               blue(job.description), job.id))

                self.fork_and_perform_job(job)

                did_perform_work = True
        finally:
            if not self.is_horse:
                self.register_death()
        return did_perform_work
