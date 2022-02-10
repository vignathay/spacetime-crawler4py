from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger, get_valid_domain
import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, frontier_url_lock):
        Thread.__init__(self)
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.frontier_url_lock =frontier_url_lock
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests from scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        while True:
            self.frontier_url_lock.acquire()
            tbd_url = self.frontier.get_tbd_url()
            self.frontier.added_count+=1
            self.frontier_url_lock.release()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"count {self.frontier.added_count} Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp, self.config)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
            domain = get_valid_domain(tbd_url, self.config.domains)
            self.frontier.domain_locks[domain] = False
