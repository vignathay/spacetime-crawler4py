import os
import logging
from hashlib import sha256
from urllib.parse import urlparse

def get_logger(name, filename=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_urlhash(url):
    parsed = urlparse(url)
    # everything other than scheme.
    return sha256(
        f"{parsed.netloc}/{parsed.path}/{parsed.params}/"
        f"{parsed.query}/{parsed.fragment}".encode("utf-8")).hexdigest()

def get_domain(url):
    parsed = urlparse(url)
    return parsed.netloc

def get_valid_domain(url, domains):
    cur_domain = get_domain(url)
    for domain in domains:
        if domain in cur_domain:
            return domain
    return None

def is_valid_domain(url, domains):
    cur_domain = get_domain(url)
    for domain in domains:
        if domain in cur_domain:
            return True
    return False


def get_parts(url):
    return urlparse(url)

def normalize(url):
    if url.endswith("/"):
        return url.rstrip("/")
    return url
