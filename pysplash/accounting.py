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
        self.current_workers = 0
        self.worker_samples = 0
        self.last_log = time.time()
        self.exited_count = 0
        self.waiting_terminate = 0

    def start(self):
        self.start_time = time.time()


    def set_workers(self, workers_count):
        if workers_count > self.max_workers:
            self.max_workers = workers_count

        self.current_workers = workers_count
        self.workers_count += workers_count
        self.worker_samples += 1

    def add_zombie(self):
        self.zombie_count += 1

    def add_failed(self):
        self.failed_count += 1

    def add_exited(self):
        self.exited_count += 1

    def set_waiting_terminate(self, waiting_terminate):
        self.waiting_terminate = waiting_terminate

    def __repr__(self):
        summary = ""

        try:
            uptime = time.time() - self.start_time
            avg_workers = self.workers_count / float(self.worker_samples)

            zombies_per_second = self.zombie_count / uptime
            failed_per_second = self.failed_count / uptime
            exited_per_second = self.exited_count / uptime

            summary += "Uptime: %ss\n" % round(uptime, 2)
            summary += "Current Workers: %i\n" % self.current_workers
            summary += "Avg Workers: %s\n" % round(avg_workers, 2)
            summary += "Max Workers: %i\n" % self.max_workers
            summary += "Waiting for Termination: %i\n" % self.waiting_terminate
            summary += "Zombies: %i\n" % self.zombie_count
            summary += "Zombies/Second: %s\n" % round(zombies_per_second, 2)
            summary += "Failed: %i\n" % self.failed_count
            summary += "Failed/Second: %s\n" % round(failed_per_second, 2)
            summary += "Exited: %i\n" % self.exited_count
            summary += "Exited/Second: %s" % round(exited_per_second, 2)

        except:
            log.exception("Exception counting")
            pass

        return summary

    def log(self):
        if time.time() - self.last_log < 5:
            return

        log.info("Pool Stats:\n%s" % (self))
        self.last_log = time.time()
