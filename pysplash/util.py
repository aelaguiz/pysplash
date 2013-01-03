# -*- coding: utf-8 -*-

import time
import rq

from .log import logger
log = logger()


def wait_jobs(jobs, max_wait=None, collect_results=False):
    finished_jobs = []
    start_time = time.time()
    while jobs:
        if max_wait and (time.time() - start_time) > max_wait:
            log.error("Timed out waiting for results from %s jobs after %ss" % (
                len(jobs), time.time() - start_time))
            return False

        new_jobs = []
        for job in jobs:
            job.refresh()

            if job.status == rq.job.Status.FINISHED or\
                    job.status == rq.job.Status.FAILED:
                finished_jobs.append(job)
            else:
                new_jobs.append(job)

        jobs = new_jobs

        time.sleep(0.1)

    if collect_results:
        return [job.result for job in finished_jobs]

    return True
