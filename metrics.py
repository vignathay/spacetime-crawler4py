import re
from urllib.parse import urlparse
from urllib.parse import urljoin
from urllib.parse import urldefrag
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
import nltk
from bs4 import BeautifulSoup
from utils import is_valid_domain, get_parts
from collections import Counter 


def UrlMetrics():
    maxpagecount = 0
    maxpageurl=""
    uniquepages=set()
    subdomaincount = dict()

    with open("AllUrls.txt", 'r') as f:
        for line in f:
            pagelist = line.split(' ')
            if(int(pagelist[0])>maxpagecount):
                maxpagecount=int(pagelist[0])
                maxpageurl=pagelist[1]

            uniquepages.add(pagelist[1])

            parsed_cur_url = get_parts(pagelist[1])

            url =  f"{parsed_cur_url.scheme}://{parsed_cur_url.netloc}"
            # print(url)

            if parsed_cur_url.netloc in subdomaincount:
                subdomaincount[parsed_cur_url.netloc]+=1
            else:
                subdomaincount[parsed_cur_url.netloc]=1
            
    print("########################## METRICS ###################################")
    print("Unique pages count: ",len(uniquepages))
    # print(uniquepages)
    print("MaxPageCount: " + str(maxpagecount) + " - MaxPageUrl: " + maxpageurl)
    print("Sub Domain dict")
    print(subdomaincount)

def TokenMetrics():
    tokenlist=[]
    tokendict ={}
    with open("AllTokens.txt", 'r') as f:
        tokenlist = f.readlines()
        # print(tokenlist[0].split(', '))
        # print(dict(Counter(tokenlist[0].split(', ')))) 
    tokendict = dict(Counter(tokenlist[0].split(', ')))
    tokendict = dict(sorted(tokendict.items(), key=lambda x: x[1], reverse=True))
    # tokendict.pop('', None)
    # print(tokendict)

    count=0
    for key in tokendict:
        if(count==50):
            break
        if(ord(key[0]) == 39 and ord(key[1]) == 39):
            continue
        print(key,tokendict[key])
        count+=1


if __name__ == "__main__":
    UrlMetrics()
    TokenMetrics()