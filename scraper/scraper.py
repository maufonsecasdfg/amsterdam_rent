import httpx
import json
from bs4 import BeautifulSoup
import random
import time
import pandas as pd
from datetime import date, datetime
import re
import chardet
import logging
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from google.cloud import bigquery
from zoneinfo import ZoneInfo
import time
import gc
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

max_tries = 5

with open('config/bigquery_config.json', 'r') as f:
    bigquery_config = json.load(f)

class Scraper():

    def __init__(self):
        self.properties = pd.DataFrame()
        
    def reset_property_table(self):
        del self.properties
        gc.collect()
        self.properties = pd.DataFrame()
        logging.info(f'Property DataFrame has been reset.')
        
    def process_price(self, price_text):
        try:
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
        except:
            return None, None
    

    def scrape_pararius(self, city, post_type, property_type, page):
        if post_type == 'Rent' and property_type == 'House':
            return None, True
        if post_type == 'Buy':
            if property_type == 'Apartment':
                typ = 'appartement'
            elif property_type == 'House':
                typ = 'huis'
            base_url = f'https://www.pararius.nl/koopwoningen/{city}/{typ}/page-{page}'
        elif post_type == 'Rent':
            base_url = f'https://www.pararius.com/apartments/{city}/page-{page}'
     
        tries = 0
        err = None
        while tries <= max_tries:
            headers = self.generate_headers()
            try:
                response = httpx.get(base_url, headers=headers, follow_redirects=True)
            except httpx.TimeoutException as errt:
                logging.info('Timeout Error, retrying...')
                err = errt
                tries += 1
                time.sleep(1 + 10*random.random())
                continue
            except httpx.ConnectError as errc:
                logging.info('Connection Error, retrying...')
                err = errc
                tries += 1
                time.sleep(1 + 10*random.random())
                continue
            if page != 1 and response.url == base_url[:-1]:
                logging.info('Redirected, trying again')
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
        if tries == max_tries:
            if err is None:
                logging.info('Exceded number of tries.')
                return None, True
            else:
                logging.info(f'Tries exceeded with error: {err}')
                return None, True
            
        response.encoding = 'utf-8' 
        soup = BeautifulSoup(response.text,'html.parser')
        page_propts = soup.find_all('li', class_="search-list__item search-list__item--listing")
        total_pages = None
        if page == 1:
            try:
                pagination = soup.find_all('a', class_="pagination__link")
                total_pages = int(pagination[-2].text)
            except:
                logging.info(f'No pages found.')
                return None, True

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
                    'bedrooms': [],
                    'furnished': [],
                    'url': [],
                    'status': []
                }
            for p in page_propts:
                data['page_source'].append('Pararius')
                data['scrape_date'].append(date.today())
                data['city'].append(city.capitalize())
                data['post_type'].append(post_type)
                data['bedrooms'].append(None)
                data['status'].append('Available')
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
            if len(df) != 0:
                self.properties = pd.concat([self.properties,df]).reset_index(drop=True)
            time.sleep(1 + 10*random.random())
            if total_pages:
                return total_pages, False
            else:
                return None, False
        else:
            logging.info('Reached page with no properties.')
            return None, True
                
    def scrape_funda(self, city, post_type, property_type, num_rooms, page, scrape_unavailable=False):
        if property_type == 'House' and num_rooms == 1:
            return None, True
        if post_type == 'Rent':
            base_url = f'https://www.funda.nl/zoeken/huur?selected_area=["{city}"]&object_type=["{property_type.lower()}"]&rooms="{num_rooms}-{num_rooms}"'
        elif post_type == 'Buy':
            base_url = f'https://www.funda.nl/zoeken/koop?selected_area=["{city}"]&object_type=["{property_type.lower()}"]&rooms="{num_rooms}-{num_rooms}"'
        if scrape_unavailable:
            base_url += '&availability=["available","negotiations","unavailable"]'
        base_url += f'&search_result={page}'

        options = webdriver.ChromeOptions()
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        user_agent = self.generate_headers(only_user_agent=True)
        options.add_argument(f"user-agent={user_agent}")

        service = Service('/usr/local/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=options)
        driver = webdriver.Chrome(options=options)
        tries = 0
        err = None
        total_pages = None
        while tries <= max_tries:
            try:
                logging.info(f'URL: {base_url}')
                driver.get(base_url)
                driver.set_page_load_timeout(60)
                driver.execute_script("return document.readyState == 'complete'")
                time.sleep(5)
                
                try:
                    cookie_accept_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
                    )
                    cookie_accept_button.click()
                    print("Cookies accepted.")
                except Exception as e:
                    print("Cookie banner not found or already handled.", e)
                
                if page == 1:
                    try:
                        pagination = WebDriverWait(driver, 45).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.my-5.flex.items-center'))
                        )
                        pagination_html = pagination.get_attribute('innerHTML')
                        soup = BeautifulSoup(pagination_html, 'html.parser')

                        page_numbers = []
                        for li in soup.find_all('li'):
                            a_tag = li.find('a')
                            if a_tag and a_tag.text.strip().isdigit():
                                page_numbers.append(int(a_tag.text.strip()))

                        total_pages = max(page_numbers) if page_numbers else 1
                        print(f"Total pages: {total_pages}")
                    except:
                        total_pages = 1
                break                
                
            except httpx.TimeoutException as errt:
                logging.info('Timeout Error, retrying...')
                err = errt
                tries += 1
                time.sleep(1 + 10*random.random())
                continue
            except httpx.ConnectError as errc:
                logging.info('Connection Error, retrying...')
                err = errc
                tries += 1
                time.sleep(1 + 10*random.random())
                continue
        if tries == max_tries:
            if err is None:
                logging.info('Exceded number of tries.')
                return None, True
            else:
                logging.info(f'Tries exceeded with error: {err}')
                return None, True
        
        
        listings = driver.find_elements(By.CSS_SELECTOR, 'div.border-b.pb-3')

        if listings:
            data = {
                'page_source': [], 'scrape_date': [], 'post_type': [], 'city': [],
                'location': [], 'postcode': [], 'title': [], 'property_type': [],
                'price': [], 'price_type': [], 'surface': [], 'surface_unit': [],
                'rooms': [], 'bedrooms': [], 'furnished': [], 'url': [], 'status': []
            }

            for listing in listings:
                inner_html = listing.get_attribute('innerHTML')
                soup = BeautifulSoup(inner_html, 'html.parser')

                link_tag = soup.select_one('a[data-testid="listingDetailsAddress"]')
                if link_tag:
                    title_span = link_tag.select_one('.flex.font-semibold span.truncate')
                    title = title_span.get_text(strip=True) if title_span else None

                    relative_url = link_tag.get('href', '').strip()
                    link = f"https://www.funda.nl{relative_url}" if relative_url else None
                else:
                    title = None
                    link = None

                address_meta = soup.select_one('a[data-testid="listingDetailsAddress"] div.truncate.text-neutral-80')
                postcode, city_name = None, None
                if address_meta:
                    address_text = address_meta.get_text(strip=True)
                    parts = address_text.split(None, 1) 
                    if len(parts) == 2:
                        postcode = parts[0]
                        city_name = parts[1]
                    else:
                        postcode = address_text
                    postcode = postcode[:4] + ' ' + postcode[4:]

                price_section = soup.select_one('div.font-semibold.mt-2.mb-0 div.truncate')
                if not price_section:
                    continue
                
                raw_price_text = price_section.get_text(strip=True)
                price, price_type = self.process_price(raw_price_text)
                if price_type:
                    price_type = price_type.replace('/maand', 'per month')
                if not price:
                    continue
                
                surfaces = []
                bedrooms = None
                
                detail_spans = soup.select('ul.flex.h-8.flex-wrap.gap-4.overflow-hidden.truncate.py-1 li.flex.items-center span')
                for span in detail_spans:
                    text = span.get_text(strip=True)
                    if text.endswith('m²'):
                        try:
                            surfaces.append(int(text.split()[0]))
                        except ValueError:
                            pass
                    elif text.isdigit():
                        try:
                            if bedrooms is None:
                                bedrooms = int(text)
                        except ValueError:
                            pass
                
                surface = max(surfaces) if surfaces else None

                status_div = soup.select_one('div.absolute.left-2.top-1')
                status = "Available"

                if status_div:
                    tag_texts = [span.get_text(strip=True).lower() for span in status_div.find_all('span')]
                    
                    if any("verkocht" in txt for txt in tag_texts):
                        status = "Unavailable"
                    elif any("onder bod" in txt for txt in tag_texts):
                        status = "In negotiations"
        

                # Append to data dictionary
                data['page_source'].append('Funda')
                data['scrape_date'].append(date.today())
                data['post_type'].append(post_type)
                data['city'].append(city.capitalize() if city else 'Unknown')
                data['location'].append(None)
                data['postcode'].append(postcode)
                data['title'].append(title)
                data['property_type'].append(property_type) 
                data['price'].append(price)
                data['price_type'].append(price_type)
                data['surface'].append(surface)
                data['surface_unit'].append('m²')
                data['rooms'].append(num_rooms)
                data['bedrooms'].append(bedrooms)
                data['furnished'].append(None) 
                data['url'].append(link)
                data['status'].append(status)
                                                
            driver.quit()

            df = pd.DataFrame(data)
            if len(df) != 0:
                self.properties = pd.concat([self.properties,df]).reset_index(drop=True)
            time.sleep(1 + 10*random.random())
            if total_pages:
                return total_pages, False
            else:
                return None, False
        else:
            if page == 1:
                return None, True
            else:
                if "Geen resultaten gevonden" in driver.page_source:
                    return None, True
                else:
                    return self.scrape_funda(city, post_type, property_type, num_rooms, page, scrape_unavailable)

    def run(self, cities, sites, post_types, property_types, scrape_unavailable=False):
        overall_page_counter = 0 
        for site in sites:
            for post_type in post_types:
                for property_type in property_types:
                    for city in cities:
                        if site == 'pararius':
                            logging.info(f'Running: {site} | {post_type} | {property_type} | {city}')
                            page = 1
                            total_pages = 100 # placeholder
                            finished = False
                            tries = 0
                            while not finished and page <= total_pages:
                                logging.info(f'Page {page}/{total_pages if page > 1 else ""}')
                                try:
                                    total_pgs, finished = self.scrape_pararius(city, post_type, property_type, page)
                                except:
                                    if tries < 5:
                                        total_pgs, finished = None, False
                                        logging.info(f'Scraping page {page} failed, retrying...')
                                        tries += 1
                                    else:
                                        total_pgs, finished = None, True
                                        logging.info(f'Scraping page {page} failed, continuing')
                                        tries = 0
                                logging.info(f'Length of dataframe: {len(self.properties)}')
                                if total_pgs:
                                    total_pages = total_pgs
                                page += 1
                                tries = 0
                                if overall_page_counter >= 15:
                                    self.update_bigquery_table()
                                    overall_page_counter = 0
                                else:
                                    overall_page_counter += 1
                        if site == 'funda':
                            for num_rooms in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]:
                                logging.info(f'Running: {site} | {post_type} | {property_type} | {city} | Room filter: {num_rooms}')
                                page = 1
                                total_pages = 100 # placeholder
                                finished = False
                                tries = 0
                                while not finished and page <= total_pages:
                                    logging.info(f'Page {page}/{total_pages if page > 1 else ""}')
                                    try:
                                        total_pgs, finished = self.scrape_funda(city, post_type, property_type, num_rooms, page, scrape_unavailable)
                                    except:
                                        if tries < 5:
                                            total_pgs, finished = None, False
                                            logging.info(f'Scraping page {page} failed, retrying...')
                                            tries += 1
                                        else:
                                            total_pgs, finished = None, True
                                            logging.info(f'Scraping page {page} failed, continuing')
                                            tries = 0
                                    logging.info(f'Length of dataframe: {len(self.properties)}')
                                    if total_pgs:
                                        total_pages = total_pgs
                                    page += 1
                                    if overall_page_counter >= 15:
                                        self.update_bigquery_table()
                                        overall_page_counter = 0
                                    else:
                                        overall_page_counter += 1
                            if len(self.properties) > 0:
                                self.update_bigquery_table()

        
    def update_bigquery_table(self):
        self.properties = self.properties.drop_duplicates(subset=['url', 'post_type'])
        try:
            self.properties = self.properties.dropna(subset=['price', 'surface', 'rooms'])
            self.properties['price'] = self.properties['price'].astype(int)
            self.properties['surface'] = self.properties['surface'].astype(int)
            self.properties['rooms'] = self.properties['rooms'].astype(int)
        except:
            return None
        
        client = bigquery.Client()
        tmp_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.tmp_property"
        property_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property"
        current_date = datetime.now(tz=ZoneInfo("Europe/Amsterdam")).strftime('%Y-%m-%d')

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("page_source", "STRING"),
                bigquery.SchemaField("scrape_date", "DATE"),
                bigquery.SchemaField("post_type", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("postcode", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("property_type", "STRING"),
                bigquery.SchemaField("price", "INTEGER"),
                bigquery.SchemaField("price_type", "STRING"),
                bigquery.SchemaField("surface", "INTEGER"),
                bigquery.SchemaField("surface_unit", "STRING"),
                bigquery.SchemaField("rooms", "INTEGER"),
                bigquery.SchemaField("bedrooms", "INTEGER"),
                bigquery.SchemaField("furnished", "STRING"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("status", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        load_job = client.load_table_from_dataframe(self.properties, tmp_table_id, job_config=job_config)
        load_job.result()

        query = f"""
            MERGE `{property_table_id}` AS property
            USING `{tmp_table_id}` AS tmp
            ON property.url = tmp.url AND property.post_type = tmp.post_type
            WHEN MATCHED THEN
            UPDATE SET 
                property.last_scrape_date = DATE("{current_date}"),
                property.status = "Available"
            WHEN NOT MATCHED THEN
            INSERT (
                page_source, post_type, city, location, postcode, title, property_type, price, price_type, 
                surface, surface_unit, rooms, bedrooms, furnished, url, status, first_scrape_date, last_scrape_date
            )
            VALUES (
                tmp.page_source, tmp.post_type, tmp.city, tmp.location, tmp.postcode, 
                tmp.title, tmp.property_type, tmp.price, tmp.price_type, tmp.surface, tmp.surface_unit, tmp.rooms,
                tmp.bedrooms, tmp.furnished, tmp.url, tmp.status, DATE("{current_date}"), DATE("{current_date}")
            )
        """
        job = client.query(query)
        job.result()

        client.delete_table(tmp_table_id, not_found_ok=True)
        logging.info(f'{property_table_id} UPDATED IN BIGQUERY')
        
        self.reset_property_table()

    def generate_headers(self, only_user_agent=False):
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.198 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) Gecko/20100101 Firefox/110.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:110.0) Gecko/20100101 Firefox/110.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.205 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6597.107 Safari/537.36"
        ]
        
        if only_user_agent:
            return random.choice(user_agents)

        sec_fetch_site = random.choice(["none", "same-origin", "cross-site"])
        sec_fetch_mode = random.choice(["navigate", "cors", "no-cors", "same-origin"])
        sec_fetch_user = "?1"  

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
        
    
        
