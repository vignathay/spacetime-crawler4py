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
        if not os.path.isdir("FileDumps"):
            os.mkdir("FileDumps")
        self.url_logger = open("FileDumps/AllUrls.txt", 'a+')
        self.token_logger = open("FileDumps/AllTokens.txt", 'a+')


    def start_async(self):
        frontier_url_lock = Lock()
        url_logger_lock = Lock()
        token_logger_lock = Lock()
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, 
            frontier_url_lock = frontier_url_lock, 
            url_logger_lock=url_logger_lock, 
            token_logger_lock=token_logger_lock,
            url_logger = self.url_logger,
            token_logger = self.token_logger,
            )
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):        
        self.start_async()
        self.join()
        self.url_logger.close()
        self.token_logger.close()

    def join(self):
        for worker in self.workers:
            worker.join()
