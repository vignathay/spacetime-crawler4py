from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from threading import Lock
import os

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory
        self.url_logger = open("AllUrls.txt", 'a+')
        self.token_logger = open("AllTokens.txt", 'a+')
        self.simhash_logger = open("SimHash.txt", 'a+')

    def start_async(self):
        frontier_url_lock = Lock()
        url_logger_lock = Lock()
        token_logger_lock = Lock()
        simhash_lock = Lock()
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier,
            frontier_url_lock = frontier_url_lock,
            url_logger_lock=url_logger_lock,
            token_logger_lock=token_logger_lock,
            simhash_lock = simhash_lock,
            url_logger = self.url_logger,
            token_logger = self.token_logger,
            simhash_logger = self.simhash_logger
            )
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()
        self.url_logger.close()
        self.token_logger.close()
        self.simhash_logger.close()

    def join(self):
        for worker in self.workers:
            worker.join()
