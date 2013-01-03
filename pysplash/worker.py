# -*- coding: utf-8 -*-

import rq

from rq.worker import green, yellow, blue


class Worker(rq.Worker):
    def __init__(
        self, queues, work_callback=None, name=None, default_result_ttl=500,
            connection=None, exc_handler=None):  # noqa

        self.work_callback = work_callback

        super(Worker, self).__init__(
            queues, name, default_result_ttl, connection,
            exc_handler)

    def fork_and_perform_job(self, job):
        super(Worker, self).fork_and_perform_job(job)

        self.work_callback(job)
