import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from urllib.parse import urldefrag
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from utils.config import Config
import nltk
from bs4 import BeautifulSoup

nltk.download('punkt')
from utils import is_valid_domain, get_parts
import time

nltk.download('stopwords')
from nltk.corpus import stopwords
import hashlib

stop_words = set(stopwords.words('english'))

past_hash = []
past_hash_urls = []


def compare_simhash(lock, fp, delay, res, url):
    filtered = False
    past_url = ""
    similarity = 0
    for i in range(len(past_hash) - 1, -1, -1):
        similarity = 1 - '{0:80b}'.format(res ^ past_hash[i]).count("1") / 128.0
        if similarity > 0.95:  # 95% similar
            # print("Pages are similar with score: ", similarity, " similar url's are: ", url, " ;;; ", past_hash_urls[i])
            filtered = True
            past_url = past_hash_urls[i]
            break
    while lock.locked():
        time.sleep(delay)
        continue
    lock.acquire()
    if filtered:
        # print("writing to file")
        fp.write("Pages are similar with score: " + str(similarity) + ", similar url's are: " + url + " ;;; " + past_url + "\n")
        lock.release()
        return False
    if len(past_hash) >= 50:
        del past_hash[0]
        del past_hash_urls[0]
    past_hash.append(res)
    past_hash_urls.append(url)
    lock.release()
    return True


def check_simhash(lock, fp, delay, tokens, url):
    hash_ls = []
    for token in tokens:
        hash_ls.append(hashlib.md5(token.encode()))

    hash_int_ls = []
    for hash in hash_ls:
        hash_int_ls.append(int(hash.hexdigest(), 16))

    res = 0
    for i in range(128):
        sum_ = 0
        for h in hash_int_ls:
            if h >> i & 1 == 1:
                sum_ += 1
            else:
                sum_ += -1
        if sum_ > 1:
            sum_ = 1
        else:
            sum_ = 0

        res += sum_ * 2 ** i
    return compare_simhash(lock, fp, delay, res, url)


def scraper(url, resp, config, url_logger, url_logger_lock, token_logger, token_logger_lock, simhash_logger,
            simhash_lock):
    links = extract_next_links(url, resp, config, url_logger, url_logger_lock, token_logger, token_logger_lock,
                               simhash_logger, simhash_lock)

    return links


def extract_next_links(url, resp, config, url_logger, url_logger_lock, token_logger, token_logger_lock, simhash_logger,
                       simhash_lock):
    if resp.status != 200:
        print("Incorrect response")
        return list()
    html = resp.raw_response.content
    soup = BeautifulSoup(html, 'html.parser')
    urls_extracted = set()
    fdist = {}

    data = soup.get_text()
    tokens = word_tokenize(data)
    # adding unique url's to a log file
    lock_and_write(url_logger, str(len(tokens)) + ' ' + url + '\n', url_logger_lock, config.frontier_pool_delay)

    filtered_tokens = []
    for token in tokens:
        token = re.sub(r'[^\x00-\x7F]+', '', token)
        token = token.lower()
        if ((token not in stop_words) and re.match(r"[a-zA-Z0-9@#*&']{2,}", token)):
            filtered_tokens.append(token)

    if len(filtered_tokens) < 30:  # avoiding less information sites
        return list()

    if not check_simhash(simhash_lock, simhash_logger, config.frontier_pool_delay, filtered_tokens,
                         url):  # avoiding similar pages
        return list()

    lock_and_write(token_logger, ", ".join(filtered_tokens), token_logger_lock, config.frontier_pool_delay)

    for link in soup.find_all('a'):
        path = link.get('href')
        if path is not None:
            if path.startswith('/'):
                path = urljoin(url, path)
            path = urldefrag(path).url  # defragment the URL
            urls_extracted.add(path)
            parsed_url = urlparse(path)
            text_file_save = str(parsed_url.netloc + parsed_url.path).replace('/', '_')

    return clean_and_filter_urls(list(urls_extracted), url, config.domains)


def clean_and_filter_urls(urls, curUrl, domains):
    list = []
    parsed_cur_url = get_parts(curUrl)
    for url in urls:
        if url is None:
            continue
        if (url.startswith('/')):
            url = f"{parsed_cur_url.scheme}://{parsed_cur_url.netloc}{url}"
        url = url.split('#')[0]
        if len(url) == 0 or not is_valid_domain(url, domains):
            continue
        if not is_valid(url):
            continue
        list.append(url)
    return list


def lock_and_write(fp, text, lock, delay):
    while lock.locked():
        time.sleep(delay)
        continue
    lock.acquire()
    fp.write(text)
    lock.release()


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        # check seed_url hostnames for validity
        # check if fragme

        if parsed.scheme not in set(["http", "https"]):
            return False
        if 'ical=' in url:  # Downloding calendar file
            return False
        if re.search(r"pdf", parsed.path.lower()):  # pdf file
            return False
        if 'wics.ics.uci.edu' in parsed.hostname and (re.search('/events', url) or 'eventDate' in url):  # events
            return False
        if 'grape.ics.uci.edu' in parsed.hostname and '/wiki/' in url:
            return False
        if 'mt-live.ics.uci.edu' in parsed.hostname and ('/events/' in url or 'eventDate' in url):
            return False
        if 'mt-live.ics.uci.edu' in parsed.hostname and 'people' in parsed.path.lower():
            return False
        if 'archive.ics.uci.edu' in parsed.hostname and '/ml/dataset' in url:  # datasets
            return False
        if 'cbcl.ics.uci.edu' in parsed.hostname and ('do=' in url or '/data' in url or '/contact' in url):
            return False
        if 'evoke.ics.uci.edu' in parsed.hostname and (
                'replytocom' in url or 'comment' in url):  # comment threads and replies
            return False
        if 'swiki.ics.uci.edu' in parsed.hostname:
            return False
        if 'sli.ics.uci.edu' in parsed.hostname and 'download' in url:  # download dataset
            return False
        if re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico" + r"|png|tiff?|mid|mp2|mp3|mp4" + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ppsx" + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names" + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso" + r"|epub|dll|cnf|tgz|sha1|tar.gz" + r"|thmx|mso|arff|rtf|jar|csv" + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
                parsed.path.lower()):
            return False
        if re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico" + r"|png|tiff?|mid|mp2|mp3|mp4" + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ppsx" + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names" + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso" + r"|epub|dll|cnf|tgz|sha1|tar.gz" + r"|thmx|mso|arff|rtf|jar|csv" + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
                parsed.query.lower()):
            return False
        return True

    except TypeError:
        print("TypeError for ", parsed)
        raise
