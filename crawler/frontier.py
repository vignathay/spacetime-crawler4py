import os
import shelve
import time

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize, get_domain
from scraper import is_valid
class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = {}
        self.domain_locks = {}
        for domain in config.domains:
            self.to_be_downloaded[domain] = list()
            self.domain_locks[domain] = False
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)
    
    def get_domain_locks(self):
        return self.domain_locks

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                domain = get_domain(url)
                self.to_be_downloaded[domain].append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            while(True):
                count = 0
                for domain in self.config.domains:
                    if not self.domain_locks[domain]:
                        if(count == len(self.config.domains)):
                            return None
                        if(len(self.to_be_downloaded[domain]) ==0):
                            count+=1
                            continue
                        self.domain_locks[domain] = True
                        return self.to_be_downloaded[domain].pop()
                time.sleep(self.config.frontier_pool_delay)                
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url)
        domain = get_domain(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded[domain].append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
