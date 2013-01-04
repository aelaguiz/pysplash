# -*- coding: utf-8 -*-

import pysplash
import rq
import redis
import uuid
import time
import random

log = pysplash.log.logger()

queue_name = str(uuid.uuid4())


def do_work():
    # Randomly crash
    if random.random() < 0.2:
        a = 5
        a /= 0

    start_time = time.time()

    a = 2**2**30

    end_time = time.time()

    return end_time - start_time


if __name__ == "__main__":
    pysplash.log.set_debug(True)

    con = redis.StrictRedis()

    with rq.Connection(con):
        q = rq.Queue(queue_name)

        for i in range(1000):
            job = q.enqueue_call(do_work)

        p = pysplash.Pool([queue_name], scale_frequency=2., zombie_timeout=30)

        log.info("Starting pool")

        p.start()
