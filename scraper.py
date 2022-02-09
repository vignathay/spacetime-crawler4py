import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from utils import get_domain, get_parts

def scraper(url, resp, domains):
    links = extract_next_links(url, resp, domains)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp, domains):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if not is_valid_page(resp):
        return []

    html_text = resp.raw_response.content
    soup = BeautifulSoup(html_text, "html.parser").find_all("a")
    links = clean_and_filter_urls([link.get("href") for link in soup], url, domains)
    return links

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

def is_valid_page(resp):
    if(resp.status>400):
        return False
    return True

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
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
