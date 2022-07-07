import pandas as pd
import numpy as np
import requests
from tqdm.notebook import tqdm

from KEYS import YELP_API_KEY

class YelpScraper():
    def __init__(self):
        self.known_bus = pd.read_csv('./data/yelp_businesses.csv')
        self.cache = pd.read_csv('./data/yelp_cache.csv')
        self.new_cache = pd.DataFrame()
        self.new_bus = pd.DataFrame()
        self.coords = pd.read_csv('./data/postcode_coordinates.csv')

    def get_businesses(self,by='postcode',radius=2000):
        '''
            by: Use either 'postcode' to use complete porstcode (ex: 1014SC) or
                'reduced_postcode' to use the first 4 numbers instead (ex: 1014)
        '''
        self.headers = {
            'Authorization': f'Bearer {YELP_API_KEY}'
        }
        base_url = 'https://api.yelp.com/v3/businesses/search'
        categories = ['active','arts','auto','beautysvc','bicycles','education',
        'eventservices','financialservices','food','health','homeservices',
        'hotelstravel','localflavor','localservices','massmedia','nightlife',
        'pets','professional','publicservicesgovt','religiousorgs','restaurants','shopping']
        if by == 'postcode':
            lookup = self.coords
        elif by == 'reduced_postcode':
            lookup = self.coords.copy()
            lookup['postcode'] = lookup['postcode'].str.replace(r'[^0-9]+','',regex=True)
            lookup = lookup.groupby('postcode').mean().reset_index()
        for postcode in tqdm(lookup['postcode']):
            lat = lookup[lookup['postcode']==postcode]['latitude'].values[0]
            lng = lookup[lookup['postcode']==postcode]['longitude'].values[0]

            for cat in categories:
                if cat not in self.cache[self.cache['postcode']==postcode]['category'].values:
                    tries = 0
                    while tries < 3:
                        query = f'{base_url}?categories={cat}&latitude={lat}&longitude={lng}&radius={radius}&limit=50'
                        result = requests.get(query,headers=self.headers,verify='./consolidate.pem')
                        if str(result.status_code)[0] == '2':
                            break
                        if result.status_code == 429:
                            print('Rate Limit Reached.')
                            return None
                        tries += 1
                    if str(result.status_code)[0] == '2':
                        busss = result.json()['businesses']
                        if len(busss) != 0:
                            df = pd.DataFrame(busss)
                            if 'price' in df.columns:
                                df = df[['id','name','url','coordinates','review_count','rating','price']]
                            else:
                                df = df[['id','name','url','coordinates','review_count','rating']]
                                df['price'] = np.nan
                            df = df.join(df['coordinates'].apply(pd.Series))
                            df = df.drop(columns='coordinates')
                            df['category'] = cat
                            self.new_bus = pd.concat([self.new_bus,df])
                    cache = pd.DataFrame({'postcode':[postcode],'category':[cat]})
                    self.new_cache = pd.concat([self.new_cache,cache])
                    
        self.new_bus = self.new_bus.drop_duplicates()

    def save_new_bus(self):
        if (list(self.known_bus.columns) == ['id', 'name', 'url', 'review_count', 'rating', 'price', 'latitude','longitude', 'category'] 
            and list(self.new_bus.columns) == ['id', 'name', 'url', 'review_count', 'rating', 'price', 'latitude','longitude', 'category']
            and list(self.cache.columns) == ['postcode', 'category']
            and list(self.New_cache.columns) == ['postcode', 'category']):
            self.known_bus = pd.concat([self.known_bus,self.new_bus]).drop_duplicates().reset_index(drop=True)
            self.known_bus.to_csv('./data/yelp_businesses.csv',index=False)
            self.cache = pd.concat([self.cache,self.new_cache]).drop_duplicates().reset_index(drop=True)
            self.cache.to_csv('./data/yelp_cache.csv',index=False)
            self.new_bus = pd.DataFrame()
        else:
            print('Either known_bus, new_bus, cache or new cache dataframe has incorrect schema.')

