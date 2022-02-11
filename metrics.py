

import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from urllib.parse import urldefrag
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
import nltk
from bs4 import BeautifulSoup
from utils import is_valid_domain, get_parts

def UrlMetrics():
    maxpagecount = 0
    maxpageurl=""
    uniquepages=set()
    subdomaincount = dict()

    with open("FileDumps/AllUrls.txt", 'r') as f:
        for line in f:
            pagelist = line.split(' ')
            if(int(pagelist[0])>maxpagecount):
                maxpagecount=int(pagelist[0])
                maxpageurl=pagelist[1]

            uniquepages.add(pagelist[1])

            parsed_cur_url = get_parts(pagelist[1])

            url =  f"{parsed_cur_url.scheme}://{parsed_cur_url.netloc}"
            print(url)

            if parsed_cur_url.netloc in subdomaincount:
                subdomaincount[parsed_cur_url.netloc]+=1
            else:
                subdomaincount[parsed_cur_url.netloc]=1
            

    print("Unique pages count: ",len(uniquepages))
    print(uniquepages)
    print("MaxPageCount: " + str(maxpagecount) + " - MaxPageUrl: " + maxpageurl)
    print(subdomaincount)


UrlMetrics()