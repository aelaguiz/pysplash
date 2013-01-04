import time
from .log import logger

log = logger()


class PoolAccounting:
    def __init__(self):
        self.start_time = time.time()
        self.zombie_count = 0
        self.workers_count = 0
        self.max_workers = 0
        self.failed_count = 0
        self.worker_samples = 0
        self.last_log = time.time()

    def start(self):
        self.start_time = time.time()


    def set_workers(self, workers_count):
        if workers_count > self.max_workers:
            self.max_workers = workers_count

        self.workers_count += workers_count
        self.worker_samples += 1

    def add_zombie(self):
        self.zombie_count += 1

    def add_failed(self):
        self.failed_count += 1

    def __repr__(self):
        summary = ""

        try:
            uptime = time.time() - self.start_time
            avg_workers = self.workers_count / float(self.worker_samples)

            zombies_per_second = self.zombie_count / uptime
            failed_per_second = self.failed_count / uptime

            summary += "Uptime: %ss\n" % round(uptime, 2)
            summary += "Avg Workers: %s\n" % round(avg_workers, 2)
            summary += "Max Workers: %i\n" % self.max_workers
            summary += "Zombies: %i\n" % self.zombie_count
            summary += "Zombies/Second: %s\n" % round(zombies_per_second, 2)
            summary += "Failed: %i\n" % self.failed_count
            summary += "Failed/Second: %s" % round(failed_per_second, 2)

        except:
            log.exception("Exception counting")
            pass

        return summary

    def log(self):
        if time.time() - self.last_log < 5:
            return

        log.info("Pool Stats:\n%s" % (self))
        self.last_log = time.time()
