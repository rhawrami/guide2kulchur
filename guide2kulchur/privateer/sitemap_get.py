import requests
from bs4 import BeautifulSoup
import json
import re
import time
import random
import gzip

'''
Okay, unfortunately, sitemap is a bit outdated (GODDAMN IT), 
but we may still be able to use this.
'''


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
}

def get_gzmf_urls(gzmf='',
                  yr_range=list(range(2020,2026))):
    '''returns list of URLs from gzipped sbjct-specific sitemap xml'''
    gzmf = gzmf.strip()
    r = requests.get(gzmf,headers=headers)
    dcgzmf = gzip.decompress(r.content)
    soup = BeautifulSoup(dcgzmf,'xml')
    urls = []
    for url in soup.find_all('url'):
        lc = url.find('loc')
        lstmd = url.find('lastmod')
        if lc and lstmd:
            lc = lc.text.strip()
            lstmd = lstmd.text.strip()
            if lstmd[:4] not in [str(i) for i in yr_range]: # only urls updated in past n years
                    continue
            else:
                urls.append((lc,lstmd))
                print(lc,lstmd) 
    return urls

def get_sitemap_urls(fpath='data/raw/sitemaps.json',
                     yr_range=list(range(2020,2026)))->dict:
    '''downloads all available URLs, will help speed up scraping :)'''
    with open(fpath,'r') as smjson:
        sm = json.load(smjson)
    sm_urls = sm['url']
    
    sm_url_dict = {}
    for sm in sm_urls:
        sbjct = re.sub(r'https://www.goodreads\.com/siteindex\.|\.xml','',sm) # get subject name, ie. 'quote'
        if sbjct not in ['author']:
            continue
        r = requests.get(sm,headers=headers)
        soup = BeautifulSoup(r.text,'xml')
        urls = []
        for i in soup.find_all('sitemap'):
            smgzf = i.find('loc') # gzipped file of urls
            lastmod = i.find('lastmod') # last modified
            if smgzf and lastmod:
                lastmod = lastmod.text.strip()
                smgzf = smgzf.text.strip()
                if lastmod[:4] not in [str(i) for i in yr_range]: # only urls updated in past n years
                    continue
                else:
                    locs = get_gzmf_urls(gzmf=smgzf,yr_range=yr_range)
                    urls.extend(locs)
                    time.sleep(random.uniform(0,1))

        sm_url_dict[sbjct] = urls
        
        time.sleep(random.randint(1,5))
    
    return sm_url_dict
        

if __name__=='__main__':
    d = get_sitemap_urls()
    for a,b in d.items():
        pass
        # print(a,b)
        print(len(b))