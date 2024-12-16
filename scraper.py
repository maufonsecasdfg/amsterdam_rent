import httpx
import json
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
from datetime import date
import re
import chardet
from google.cloud import secretmanager
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


with open('config/bigquery_config.json', 'r') as f:
    bigquery_config = json.load(f)

class Scraper():

    def __init__(self):
        self.properties = pd.DataFrame()
        
    def process_price(self, price_text):
        pattern = r'.*\d[\d.,]*\s*-\s*\d[\d.,]*.*' # price range pattern
        price_text = price_text.replace('€','').replace('\n','')
        if bool(re.match(pattern, price_text)):
            num1str = re.sub(r'[^\d]', '', price_text.split('-')[0])
            num2str = re.sub(r'[^\d]', '', price_text.split('-')[1])
            num1 = int(num1str)
            num2 = int(num2str)
            price = int((num1+num2)/2)
            price_type = ' '.join(price_text.replace('.','')
                                  .replace(',','')
                                  .replace(num1str,'')
                                  .replace(num2str,'')
                                  .strip()
                                  .split(' ')[1:]).strip()
        else:
            price = int(re.sub(r'[^\d]', '', price_text))
            price_type = ' '.join(price_text.strip().split(' ')[1:])
        return price, price_type    
    
    def get_proxies_from_secret(self, secret_name):
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{bigquery_config['project_id']}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return json.loads(response.payload.data.decode("UTF-8"))

    def get_random_proxy(self, secret_name):
        proxies = self.get_proxies_from_secret(secret_name)
        return random.choice(proxies)

    def scrape(self, city, site, post_type, max_pages=10000):
        types = []
        if post_type == 'Buy':
            types = ['appartement','huis','studio']
        if site == 'pararius':
            for typ in types:
                if post_type == 'Rent':
                    logging.info('            Running all for rent')
                    base_url = f'https://www.pararius.com/apartments/{city}/'
                elif post_type == 'Buy':
                    logging.info(f'            Running property type: {typ}')
                    base_url = f'https://www.pararius.nl/koopwoningen/{city}/{typ}/'
                page = 1
                while page <= max_pages:
                    tries = 0
                    err = None
                    while tries <= 15:
                        logging.info(f'            Page: {page}')
                        headers = self.generate_headers()
                        #proxy_list_secret_name = "proxy-list" 
                        #proxy_url = self.get_random_proxy(proxy_list_secret_name)
                        #proxies = {"http://": proxy_url, "https://": proxy_url}
                        try:
                            #response = httpx.get(base_url+f'page-{page}', headers=headers, follow_redirects=True, proxies=proxies)
                            response = httpx.get(base_url+f'page-{page}', headers=headers)
                        except httpx.TimeoutException as errt:
                            logging.info('                Timeout Error, retrying...')
                            err = errt
                            tries += 1
                            time.sleep(1 + 10*random.random())
                            continue
                        except httpx.ConnectError as errc:
                            logging.info('                Connection Error, retrying...')
                            err = errc
                            tries += 1
                            time.sleep(1 + 10*random.random())
                            continue
                        if str(response.status_code)[0] == '2':
                            detected_encoding = chardet.detect(response.content)
                            if detected_encoding['encoding'] == None:
                                logging.info('Response is corrupted')
                                tries += 1
                                time.sleep(1 + 10*random.random())
                                continue
                            break
                        else:
                            tries += 1
                            time.sleep(1 + 10*random.random())
                    if tries == 15:
                        if err is None:
                            logging.info('            Reached final page (tries route).')
                            break
                        else:
                            logging.info(f'            Tries exceeded with error: {err}')
                            page += 1
                            time.sleep(1 + 10*random.random())
                            continue
                        
                    response.encoding = 'utf-8' 
                    soup = BeautifulSoup(response.text,'html.parser')
                    page_propts = soup.find_all('li', class_="search-list__item search-list__item--listing")

                    if len(page_propts) != 0:
                        data = {'page_source': [],
                                'scrape_date': [],
                                'post_type': [],
                                'city': [],
                                'location': [],
                                'postcode': [],
                                'title': [],
                                'property_type': [],
                                'price': [],
                                'price_type': [],
                                'surface': [],
                                'surface_unit': [],
                                'rooms': [],
                                'furnished': [],
                                'url': []
                            }
                        for p in page_propts:
                            data['page_source'].append('Pararius')
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
                                data['property_type'].append(t.get_text().replace('\n','').strip().split(' ')[0].strip())
                                data['url'].append('https://www.pararius.nl' + t.attrs['href'])
                            else:
                                data['title'].append(None)
                                data['property_type'].append(None)
                                data['url'].append(None)
                            pr = p.find('div',class_="listing-search-item__price")
                            if pr is not None:
                                try:
                                    price, price_type = self.process_price(pr.get_text())
                                    data['price'].append(price)
                                    data['price_type'].append(price_type)
                                except Exception as e:
                                    logging.info(e)
                                    data['price'].append(None)
                                    data['price_type'].append(None)
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
                        time.sleep(1 + 10*random.random())
                        page += 1
                    else:
                        logging.info('            Reached final page (no properties route).')
                        break

    def run(self, cities, sites, post_types, max_pages=10000):
        cities, sites, post_types
        for site in sites:
            logging.info(f'Running Site: {site}')
            for post_type in post_types:
                logging.info(f'    Running post types: {post_type}')
                for city in cities:
                    logging.info(f'        Running city: {city}')
                    self.scrape(city, site, post_type, max_pages)
        self.properties = self.properties.drop_duplicates()
        self.properties = self.properties.dropna(subset=['price', 'surface', 'rooms'])
        self.properties['price'] = self.properties['price'].astype(int)
        self.properties['surface'] = self.properties['surface'].astype(int)
        self.properties['rooms'] = self.properties['rooms'].astype(int)

    def generate_headers(self):
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.198 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:110.0) Gecko/20100101 Firefox/110.0",
        ]

        # Sec-Fetch headers typically reflect browser behavior.
        # You can keep them stable for now to seem more like a normal browser.
        sec_fetch_site = random.choice(["none", "same-origin", "cross-site"])
        sec_fetch_mode = random.choice(["navigate", "cors", "no-cors", "same-origin"])
        sec_fetch_user = "?1"  # This header often appears as "?1" when a human navigates

        headers = {
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "DNT": "0",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": random.choice(user_agents),
            "Sec-Fetch-User": sec_fetch_user,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Site": sec_fetch_site,
            "Sec-Fetch-Mode": sec_fetch_mode
        }

        return headers
        
    
        
