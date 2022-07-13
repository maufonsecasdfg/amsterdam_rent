import httpx
from bs4 import BeautifulSoup
from random import random, randint
import time
import pandas as pd

class Scraper():

    def __init__(self):
        self.properties = pd.DataFrame()

    def scrape(self, city, site, postType, max_pages):
        properties = []
        if site == 'pararius':
            for typ in ['appartement','huis','studio']:
                if postType == 'Rent':
                    print('            Running all for rent')
                    base_url = f'https://www.pararius.com/apartments/{city}/'
                elif postType == 'Buy':
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
                            response = httpx.get(base_url+f'page-{page}', verify='./consolidate.pem', headers=headers, follow_redirects=True)
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
                                'postType': [],
                                'city': [],
                                'postcode': [],
                                'type': [],
                                'price': [],
                                'surface': [],
                                'rooms': [],
                                'furnished': [],
                                'latitude': [],
                                'longitude': []
                            }
                        for p in page_propts:
                            data['source'].append('Pararius')
                            data['city'].append(city)
                            data['postType'].append(postType)
                            pc = p.find('div',class_="listing-search-item__location")
                            if pc is not None:
                                data['postcode'].append(pc.get_text().replace('  ','').replace('\n',''))
                            else:
                                data['postcode'].append(None)
                            t = p.find('a',class_='listing-search-item__link listing-search-item__link--title')
                            if t is not None:
                                data['type'].append(t.get_text().split(' ')[0])
                            else:
                                data['type'].append(None)
                            pr = p.find('div',class_="listing-search-item__price")
                            if pr is not None:
                                data['price'].append(pr.get_text().replace('  ','').replace('\n',''))
                            else:
                                data['price'].append(None)
                            sf = p.find('li',class_="illustrated-features__item illustrated-features__item--surface-area")
                            if sf is not None:
                                data['surface'].append(sf.get_text())
                            else:
                                data['surface'].append(None)
                            rm = p.find('li',class_="illustrated-features__item illustrated-features__item--number-of-rooms")
                            if rm is not None:
                                data['rooms'].append(rm.get_text())
                            else:
                                data['rooms'].append(None)
                            fr = p.find('li',class_="illustrated-features__item illustrated-features__item--interior")
                            if fr is not None:
                                data['furnished'].append(p.find('li',class_="illustrated-features__item illustrated-features__item--interior").get_text())
                            else:
                                data['furnished'].append(None)
                            data['latitude'].append(None)
                            data['longitude'].append(None)

                        df = pd.DataFrame(data)
                        self.properties = pd.concat([self.properties,df]).reset_index(drop=True)
                        time.sleep(1 + 5*random())
                        page += 1
                    else:
                        print('            Reached final page.')
                        break

        elif site == 'funda':
            for typ in ['woonhuis','appartment']:
                print(f'            Running property type: {typ}')
                if postType == 'Rent':
                    postType_funda = 'huur'
                elif postType == 'Buy':
                    postType_funda = 'koop'
                base_url = f'https://www.funda.nl/en/{postType_funda}/{city}/{typ}/'
                page = 1
                while page <= max_pages:
                    print(f'            Page: {page}')
                    headers = self.generate_headers()
                    tries = 0
                    err = None
                    while tries <= 3:
                        try:
                            response = httpx.get(base_url+f'p{page}',verify='./consolidate.pem', headers=headers, follow_redirects=True)
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
                    page_propts = soup.find_all('div', class_="search-result-content-inner")

                    if len(page_propts) != 0:
                        data = {'source': [],
                                'postType': [],
                                'city': [],
                                'postcode': [],
                                'type': [],
                                'price': [],
                                'surface': [],
                                'rooms': [],
                                'furnished': [],
                                'latitude': [],
                                'longitude': []
                            }
                        for p in page_propts:
                            data['source'].append('Funda')
                            data['city'].append(city)
                            data['postType'].append(postType)
                            pc = p.find('h4', class_='search-result__header-subtitle fd-m-none')
                            if pc is not None:
                                data['postcode'].append(p.find('h4', class_='search-result__header-subtitle fd-m-none').get_text().replace('\r','').replace('\n','').replace('  ',''))
                            else:
                                data['postcode'].append(None)
                            if typ == 'woonhuis':
                                data['type'].append('House')
                            else:
                                data['type'].append('Apartment')
                            pr = p.find('span', class_='search-result-price')
                            if pr is not None:
                                data['price'].append(pr.get_text())
                            else:
                                data['price'].append(None)
                            sf = p.find('span', title='Living area')
                            if sf is not None:
                                data['surface'].append(sf.get_text())
                            else:
                                data['surface'].append(None)
                            r = [x for x in p.find_all('li') if 'room' in x.get_text()]
                            if len(r) > 0:
                                data['rooms'].append(r[0].get_text())
                            else:
                                data['rooms'].append(None)
                            data['furnished'].append(None)
                            data['latitude'].append(None)
                            data['longitude'].append(None)
                        
                        df = pd.DataFrame(data)
                        self.properties = pd.concat([self.properties,df]).reset_index(drop=True)
                        time.sleep(1 + 5*random())
                        page += 1
                    else:
                        print('            Reached final page.')
                        break
        
        elif site == 'kamernet':
            if postType == 'Buy':
                print('            No buy option in Kamernet')
            else:
                base_url = f'https://kamernet.nl/en/for-rent/rooms-{city}'
                page = 1
                while page <= max_pages:
                    print(f'            Page: {page}')
                    headers = self.generate_headers()
                    tries = 0
                    err = None
                    while tries <= 3:
                        try:
                            response = httpx.get(base_url+f'?pageno={page}',verify='./consolidate.pem', headers=headers, follow_redirects=True)
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
                        if str(response.status_code)[0] == '4':
                            break
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

                    if str(response.status_code)[0] == '4':
                        break

                    soup = BeautifulSoup(response.text,'html.parser')
                    page_propts = soup.find_all('div', class_="tile-wrapper ka-tile")

                    if len(page_propts) != 0:
                        data = {'source': [],
                                'postType': [],
                                'city': [],
                                'postcode': [],
                                'type': [],
                                'price': [],
                                'surface': [],
                                'rooms': [],
                                'furnished': [],
                                'latitude': [],
                                'longitude': []
                            }
                        for p in page_propts:
                            data['source'].append('Kamernet')
                            data['city'].append(city)
                            data['postType'].append(postType)
                            data['postcode'].append(p.find('div',class_='tile-data').find('meta').get('content'))
                            data['type'].append(p.find('div',class_='tile-data').find('div',class_='tile-room-type').get_text())
                            data['price'].append(p.find('div',class_='tile-data').find('div',class_='tile-rent').get_text())
                            data['surface'].append(p.find('div',class_='tile-data').find('div',class_='tile-surface').get_text())
                            if p.find('div',class_='tile-bedroom-numbers') is not None:
                                data['rooms'].append(p.find('div',class_='tile-bedroom-numbers').get_text())
                            else:
                                data['rooms'].append(None)
                            data['furnished'].append(p.find('div',class_='tile-data').find('div',class_='tile-furnished').get_text())
                            data['latitude'].append(p.find_all('meta')[-2].get('content'))
                            data['longitude'].append(p.find_all('meta')[-1].get('content'))
                        df = pd.DataFrame(data)
                        self.properties = pd.concat([self.properties,df]).reset_index(drop=True)
                        time.sleep(1 + 5*random())
                        page += 1
                    else:
                        print('            Reached final page.')
                        break

    def run(self, cities, sites, postTypes, max_pages):
        cities, sites, postTypes
        for site in sites:
            print(f'Running Site: {site}')
            for postType in postTypes:
                print(f'    Running post types: {postType}')
                for city in cities:
                    print(f'        Running city: {city}')
                    self.scrape(city, site, postType, max_pages)

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
            'Accept-Encoding':str3,
            'Accept-Language': str4
        }

        return headers
