PySplash
========

A Python RQ-Based Scaling Worker Pool

Introduction
------------
PySplash is intended to be used in a cluster environment where concurrency is high and resources are limited. It is designed to automatically scale up the number of processes on a system while not exceeding memory limitations.

For instance, here it is keeping my laptop cpu @ 80% steadily:

![80 Percent](https://raw.github.com/aelaguiz/pysplash/master/doc/pct_80.jpg)

This is an example of workers which pause for 10 seconds then go to 100% cpu. They are spun up because work is outstanding in the queue and no CPU is being used (sleeping). Once they become active, PySplash starts to spin workers down. Eventually they exit on their own (finished with work), and there are none running. Then PySplash starts to spin them up again, as the work queue has jobs outstanding - but the new ones are paused. This repeats and produces this CPU wave:

![Wave](https://raw.github.com/aelaguiz/pysplash/master/doc/wave.jpg)

Example
-------

Calculating the fibonnaci sequence:

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

	        p = pysplash.Pool([queue_name], scale_frequency=2., zombie_timeout=15)

    	    log.info("Starting pool")

        	p.start()

### License

* Authored by Amir Elaguizy <aelaguiz@gmail.com>
* Distributed under MIT License, see *LICENSE.md*