# -*- coding: utf-8 -*-

import pysplash
import rq
import redis
import uuid

log = pysplash.log.logger()

queue_name = str(uuid.uuid4())


def fib(n):
    log.debug("Fib(%s)" % n)

    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        with rq.Connection(redis.StrictRedis()):
            q = rq.Queue(queue_name)

            jobs = [
                q.enqueue_call(fib, (n-1,)),
                q.enqueue_call(fib, (n-2,))
            ]

            log.debug("Waiting for results of %s & %s" % (
                n-1, n-2))

            res = pysplash.wait_jobs(jobs, collect_results=True)

            return sum(res)


if __name__ == "__main__":
    pysplash.log.set_debug(True)

    con = redis.StrictRedis()

    with rq.Connection(con):
        q = rq.Queue(queue_name)

        job = q.enqueue_call(fib, (4,))

        p = pysplash.Pool(
            [queue_name], scale_frequency=2., zombie_timeout=15,
            retire_idle=False)

        log.info("Starting pool")

        p.start()
