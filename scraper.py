import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from urllib.parse import urldefrag
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from utils.config import Config
import nltk
import pickle
from bs4 import BeautifulSoup
nltk.download('punkt')
from utils import get_domain, get_parts


def scraper(url, resp, config):
    links = extract_next_links(url, resp, config)
    #max length of the page missing
    url_dict = {
        "urls_done":[],
        "max_length_page_url":"",
        "max_page_length":0
        }
    try:
        with open(f"FileDumps/AllUrls.pickle", 'rb') as handle:
            urls_done = pickle.load(handle)['urls_done']
            urls_done += links
            pickle.dump(urls_done, handle, protocol=pickle.HIGHEST_PROTOCOL)    
    except (OSError, IOError, EOFError) as e:
        url_dict["urls_done"] = links
        pickle.dump(url_dict, open(f"FileDumps/AllUrls.pickle", 'wb'))

    return links

def extract_next_links(url, resp, config):
    if resp.status != 200:
        print("Incorrect response")
        return list()
    html = resp.raw_response.content
    soup = BeautifulSoup(html, 'html.parser')
    urls_extracted = set()
    fdist = {}
    try:
        with open(f"FileDumps/AllTokens.pickle", 'rb') as handle:
            fdist = pickle.load(handle)
    except (OSError, IOError, EOFError) as e:
        pickle.dump(fdist, open(f"FileDumps/AllTokens.pickle", 'wb'))

    try:
        with open(f"FileDumps/AllUrls.pickle", 'rb+') as handle:
            data_loaded = pickle.load(handle)
            max_len_file = data_loaded["max_page_length"]
            print(len(html), max_len_file)
            if len(html) > max_len_file:
                data_loaded["max_length_page_url"] = url
                data_loaded["max_page_length"] = len(html)
                #print(data_loaded["max_page_length"], data_loaded["max_length_page_url"])
                pickle.dump(data_loaded,open(f"FileDumps/AllUrls.pickle", 'wb'))
    except (OSError, IOError, EOFError) as e:
        url_dict = {"urls_done":[], "max_length_page_url": url, "max_page_length":len(html)}
        pickle.dump(url_dict, open(f"FileDumps/AllTokens.pickle", 'wb'))
     
    data = soup.get_text()          
    tokens = word_tokenize(data)
    for token in tokens:
        if token in fdist.keys():
            fdist[token] += 1
        else:
            fdist[token] = 1
   
    for link in soup.find_all('a'):
        path = link.get('href')
        if path and path.startswith('/'):
            path = urljoin(url, path)
            defrag_path = urldefrag(path) #defragment the URL
            urls_extracted.add(defrag_path.url) 
            parsed_url = urlparse(defrag_path.url)
            text_file_save = str(parsed_url.netloc + parsed_url.path).replace('/','_')
            
    with open(f"FileDumps/AllTokens.pickle", 'wb') as handle:
        pickle.dump(fdist, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return clean_and_filter_urls(list(urls_extracted), url, config.domains) 

def clean_and_filter_urls(urls, curUrl, domains):
    list = []
    parsed_cur_url = get_parts(curUrl)
    for url in urls:
        if url is None:
            continue
        if(url.startswith('/')):
            url =  f"{parsed_cur_url.scheme}://{parsed_cur_url.netloc}{url}"
        url = url.split('#')[0]
        if len(url) == 0 or get_domain(url) not in domains:
            continue
        list.append(url)
    return list


def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        #check seed_url hostnames for validity
        #check if fragme
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
