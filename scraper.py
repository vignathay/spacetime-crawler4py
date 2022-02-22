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
            return False    #https://wics.ics.uci.edu/event/fall-2021-week-4-resume-workshop/?ical=1
        if re.search(r"pdf", parsed.path.lower()):  # pdf file
            return False    #https://www.informatics.uci.edu/files/pdf/InformaticsBrochure-March2018
        if 'wics.ics.uci.edu' in parsed.hostname and (re.search('/events', url) or 'eventDate' in url):  # events
            return False        #https://wics.ics.uci.edu/event/fall-2021-week-4-resume-workshop/?ical=1
        if 'grape.ics.uci.edu' in parsed.hostname and '/wiki/' in url:
            return False        #https://grape.ics.uci.edu/wiki/asterix/wiki/WikiStart #https://grape.ics.uci.edu/wiki/public/wiki/cs222p-2018-fall?action=diff&version=59
        if 'mt-live.ics.uci.edu' in parsed.hostname and ('/events/' in url or 'eventDate' in url):
            return False        #https://mt-live.ics.uci.edu/event/2021-southern-california-software-engineering-symposium/?ical=1 #https://mt-live.ics.uci.edu/event/2021-southern-california-software-engineering-symposium
        if 'mt-live.ics.uci.edu' in parsed.hostname and 'people' in parsed.path.lower():
            return False        #https://mt-live.ics.uci.edu/people/?filter%5Boffices_ics%5D%5B0%5D=1082&filter%5Boffices_ics%5D%5B1%5D=1083&filter%5Boffices_ics%5D%5B2%5D=1077&filter%5Bemployee_type%5D%5B0%5D=783&filter%5Bemployee_type%5D%5B1%5D=1095&filter%5Bemployee_type%5D%5B3%5D=1094&filter%5Bresearch_areas_ics%5D%5B0%5D=1110&filter%5Bresearch_areas_ics%5D%5B2%5D=1114&filter%5Bresearch_areas_ics%5D%5B4%5D=1119&filter%5Bresearch_areas_ics%5D%5B5%5D=1880&filter%5Bresearch_areas_ics%5D%5B6%5D=1873&filter%5Bresearch_areas_ics%5D%5B7%5D=1120&filter%5Bresearch_areas_ics%5D%5B8%5D=1123&filter%5Bresearch_areas_ics%5D%5B9%5D=1132&filter%5Bresearch_areas_ics%5D%5B10%5D=1127&filter%5Bresearch_areas_ics%5D%5B11%5D=1891&filter%5Bresearch_areas_ics%5D%5B12%5D=1124&filter%5Bresearch_areas_ics%5D%5B13%5D=1900&filter%5Bresearch_areas_ics%5D%5B14%5D=1112&filter%5Bresearch_areas_ics%5D%5B15%5D=1882&filter%5Binstitutes_centers%5D%5B1%5D=1074&filter%5Binstitutes_centers%5D%5B2%5D=1072
        if 'archive.ics.uci.edu' in parsed.hostname and '/ml/dataset' in url:  # datasets
            return False #http://archive.ics.uci.edu/ml/datasets/Dodgers+Loop+Sensor
        if 'cbcl.ics.uci.edu' in parsed.hostname and ('do=' in url or '/data' in url or '/contact' in url):
            return False  #https://cbcl.ics.uci.edu/doku.php/software?do=diff&rev2%5B0%5D=1397844162&rev2%5B1%5D=1397844231&difftype=sidebyside
        if 'evoke.ics.uci.edu' in parsed.hostname and (
                'replytocom' in url or 'comment' in url):  # comment threads and replies
            return False  #https://evoke.ics.uci.edu/about/values-in-design/?replytocom=106076
        if 'swiki.ics.uci.edu' in parsed.hostname:
            return False  #https://swiki.ics.uci.edu/doku.php/hardware:laptops?idx=hardware%3Astorage
        if 'sli.ics.uci.edu' in parsed.hostname and 'download' in url:  # download dataset
            return False  #http://sli.ics.uci.edu/Classes-2008F/Main?action=download&upname=Global_Puzzle
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
