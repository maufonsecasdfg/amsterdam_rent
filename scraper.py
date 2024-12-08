import httpx
import json
from bs4 import BeautifulSoup
from random import random, randint
import time
import pandas as pd
from datetime import date
import re

class Scraper():

    def __init__(self):
        self.properties = pd.DataFrame()

    def scrape(self, city, site, post_type, max_pages=10000):
        properties = []
        if site == 'pararius':
            for typ in ['appartement','huis','studio']:
                if post_type == 'Rent':
                    print('            Running all for rent')
                    base_url = f'https://www.pararius.com/apartments/{city}/'
                elif post_type == 'Buy':
                    print(f'            Running property type: {typ}')
                    base_url = f'https://www.pararius.nl/koopwoningen/{city}/{typ}/'
                page = 1
                while page <= max_pages:
                    print(f'            Page: {page}')
                    headers = self.generate_headers()
                    tries = 0
                    err = None
                    while tries <= 3:
                        try:
                            response = httpx.get(base_url+f'page-{page}', headers=headers, follow_redirects=True)
                        except httpx.TimeoutException as errt:
                            print('                Timeout Error, retrying...')
                            err = errt
                            tries += 1
                            continue
                        except httpx.ConnectError as errc:
                            print('                Connection Error, retrying...')
                            err = errc
                            tries += 1
                            continue
                        if str(response.status_code)[0] == '2':
                            break
                        else:
                            tries += 1
                    if tries == 3:
                        if err is None:
                            print('            Reached final page.')
                            break
                        else:
                            print(f'            Tries exceeded with error: {err}')
                            page += 1
                            continue

                    soup = BeautifulSoup(response.text,'html.parser')
                    page_propts = soup.find_all('li', class_="search-list__item search-list__item--listing")

                    if len(page_propts) != 0:
                        data = {'source': [],
                                'scrape_date': [],
                                'post_type': [],
                                'city': [],
                                'location': [],
                                'postcode': [],
                                'title': [],
                                'price': [],
                                'price_type': [],
                                'surface': [],
                                'surface_unit': [],
                                'rooms': [],
                                'furnished': [],
                                'url': []
                            }
                        for p in page_propts:
                            data['source'].append('Pararius')
                            data['scrape_date'].append(date.today())
                            data['city'].append(city)
                            data['post_type'].append(post_type)
                            pc = p.find('div',class_="listing-search-item__sub-title'")
                            if pc is not None:
                                data['location'].append(pc.get_text().replace('\n', '').strip())
                                data['postcode'].append(' '.join(pc.get_text().replace('\n', '').strip().split(' ')[:2]))
                            else:
                                data['location'].append(None)
                                data['postcode'].append(None)
                            t = p.find('a',class_='listing-search-item__link listing-search-item__link--title')
                            if t is not None:
                                data['title'].append(t.get_text().replace('\n','').strip())
                                data['url'].append('https://www.pararius.nl' + t.attrs['href'])
                            else:
                                data['title'].append(None)
                                data['url'].append(None)
                            pr = p.find('div',class_="listing-search-item__price")
                            if pr is not None:
                                data['price'].append(re.sub(r'[^\d]', '', pr.get_text()))
                                data['price_type'].append(' '.join(pr.get_text().replace('€','').strip().split(' ')[1:]))
                            else:
                                data['price'].append(None)
                                data['price_type'].append(None)
                            sf = p.find('li',class_="illustrated-features__item illustrated-features__item--surface-area")
                            if sf is not None:
                                data['surface'].append(re.sub(r'[^\d]', '', sf.get_text()))
                                data['surface_unit'].append(sf.get_text().split(' ')[-1])
                            else:
                                data['surface'].append(None)
                            rm = p.find('li',class_="illustrated-features__item illustrated-features__item--number-of-rooms")
                            if rm is not None:
                                data['rooms'].append(re.sub(r'[^\d]', '', rm.get_text()))
                            else:
                                data['rooms'].append(None)
                            fr = p.find('li',class_="illustrated-features__item illustrated-features__item--interior")
                            if fr is not None:
                                data['furnished'].append(p.find('li',class_="illustrated-features__item illustrated-features__item--interior").get_text())
                            else:
                                data['furnished'].append(None)

                        df = pd.DataFrame(data)
                        self.properties = pd.concat([self.properties,df]).reset_index(drop=True)
                        time.sleep(1 + 5*random())
                        page += 1
                    else:
                        print('            Reached final page.')
                        break

    def run(self, cities, sites, post_types, max_pages=10000):
        cities, sites, post_types
        for site in sites:
            print(f'Running Site: {site}')
            for post_type in post_types:
                print(f'    Running post types: {post_type}')
                for city in cities:
                    print(f'        Running city: {city}')
                    self.scrape(city, site, post_type, max_pages)
        self.properties = self.properties.drop_duplicates()

    def generate_headers(self):

        user_agent = [
            ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'],
            ['Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'],
            ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'],
            ['Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1'],
            ['Mozilla/5.0 (Linux; U; Android 4.2.3; he-il; NEO-X5-116A Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/5.0 Safari/534.30'],
            ['Mozilla/5.0 (X11; U; Linux armv7l like Android; en-us) AppleWebKit/531.2+ (KHTML, like Gecko) Version/5.0 Safari/533.2+ Kindle/3.0+'],
            ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'],
            ['Mozilla/5.0 (iPhone; CPU iPhone OS 12_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/69.0.3497.105 Mobile/15E148 Safari/605.1']
        ]

        SecFetchUser = ['?1','?T','?F']
        SecFetchSite = ['cross-site','same-origin','same-site']
        SecFetchMode = ['websocket','no-cors','same-origin','cors']

        AcceptEncoding = [
                ['gzip','identity'],['gzip','deflate','identity'],['br','deflate','gzip'],
                ['identity','br','deflate'],['gzip','deflate']
        ]

        AcceptLanguage = [
            ['nl-NL,nl;q=0.'+str(randint(1,9)),'en-USA,en;q=0.'+str(randint(1,9))],
            ['fr-CH,fr;q=0.'+str(randint(1,9)),'nl-NL,nl;q=0.'+str(randint(1,9))],
            ['nl-NL,nl;q=0.'+str(randint(1,9)),'de;q=0.'+str(randint(1,9))],
            ['nl-NL,nl;q=0.'+str(randint(1,9)),'de-CH;q=0.'+str(randint(1,9))]
        ]

        value_fetchuser = randint(0,2)
        value_site = randint(0,2)
        value_mode = randint(0,3)
        value_userag = randint(0,7)

        str_user = ','.join(user_agent[value_userag])

        accptenco = randint(0,3)
        str3 = ','.join(AcceptEncoding[accptenco])
        accplang = randint(0,3)
        str4 = ','.join(AcceptLanguage[accplang])

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'DNT': '0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': str_user,
            'Sec-Fetch-User':SecFetchUser[value_fetchuser],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.7,image/webp,image/apng,*/*;q=0.3,application/signed-exchange;v=b3',
            'Sec-Fetch-Dest':'none',
            'Sec-Fetch-Site': SecFetchSite[value_site],
            'Sec-Fetch-Mode': SecFetchMode[value_mode],
            'Accept-Encoding': str3,
            'Accept-Language': str4
        }

        return headers

    
    
    
        
