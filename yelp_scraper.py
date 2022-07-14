import pandas as pd
import numpy as np
import requests
from tqdm.notebook import tqdm
import geopandas as gpd
from shapely import wkt

from KEYS import YELP_API_KEY

class YelpScraper():
    def __init__(self):
        try:
            self.known_bus = pd.read_csv('./data/yelp_businesses.csv')
        except FileNotFoundError as e:
            self.known_bus = pd.DataFrame(columns=['id', 'name', 'url', 'review_count', 'rating', 'price', 'latitude','longitude', 'category'])
        try:
            self.cache = pd.read_csv('./data/yelp_cache.csv')
        except FileNotFoundError as e:
            self.cache = pd.DataFrame(columns=['id', 'category'])
        self.new_cache = pd.DataFrame(columns=['id', 'category'])
        self.new_bus = pd.DataFrame(columns=['id', 'name', 'url', 'review_count', 'rating', 'price', 'latitude','longitude', 'category'])

        try:
            self.geometry = pd.read_csv('./data/citygeometry.csv')
            self.geometry['geometry'] = self.geometry['geometry'].apply(wkt.loads)
            self.geometry = gpd.GeoDataFrame(self.geometry, geometry='geometry').set_crs('EPSG:4326')
            gyelp = self.known_bus.copy()
            gyelp = gpd.GeoDataFrame(gyelp, geometry=gpd.points_from_xy(self.known_bus.longitude, self.known_bus.latitude))
            gyelp = gyelp.set_crs('EPSG:4326')
            gyelp = gpd.sjoin(gyelp,self.geometry,how='right',predicate='within')
            self.yelp_count = gyelp.drop_duplicates(subset=['id']).groupby(['gemeentenaam','wijknaam','buurtnaam'])[['id']].count().sort_values(by='id')
        except FileNotFoundError as e:
            print('Geometry not found.')
            self.geometry = None
            self.yelp_count = None

        try:
            self.latlng_grid = pd.read_csv('./data/latlng_grid.csv')
        except FileNotFoundError as e:
            self.latlng_grid = pd.DataFrame(columns=['latitude', 'longitude', 'id'])


    def generate_latlong_grid(self,res=40,knwon_bus_thrs=20,label=None):
        regen = input('Do you want to regenerate the lat-long grid? (y/n)')
        if regen == 'y':
            try:
                props =  pd.read_csv('./data/localized_data.csv')
            except FileNotFoundError as e:
                print('Localized Data not found. Cannot regenerate lat-long grid.')
                return None
            
            if self.geometry is None:
                print('No geometry was found. Cannot regenerate lat-long grid.')
                return None
            
            props = gpd.GeoDataFrame(props, geometry=gpd.points_from_xy(props.longitude, props.latitude))
            props = props.set_crs('EPSG:4326')

            props['longcat'] = pd.cut(props['longitude'],res)
            props['latcat'] = pd.cut(props['latitude'],res)

            latlng_grid = pd.DataFrame({'latcat' : props['latcat'].cat.categories}).merge(pd.DataFrame({'longcat' : props['longcat'].cat.categories}), how='cross')
            latlng_grid['latitude'] = latlng_grid['latcat'].apply(lambda x: x.mid)
            latlng_grid['longitude'] = latlng_grid['longcat'].apply(lambda x: x.mid)

            latlng_grid = gpd.GeoDataFrame(latlng_grid, geometry=gpd.points_from_xy(latlng_grid.longitude, latlng_grid.latitude)).set_crs('EPSG:4326')
            latlng_grid = gpd.sjoin(latlng_grid,self.geometry,how='left',predicate='within').dropna()

            latlng_grid = self.yelp_count[self.yelp_count['id']<knwon_bus_thrs].join(latlng_grid.set_index(['gemeentenaam','wijknaam','buurtnaam']))
            latlng_grid = gpd.GeoDataFrame(latlng_grid, geometry=gpd.points_from_xy(latlng_grid.longitude, latlng_grid.latitude)).set_crs('EPSG:4326')

            latlng_grid = latlng_grid.reset_index().dropna()
            self.latlng_grid = latlng_grid[['latitude','longitude']]
            if label is not None:
                self.latlng_grid['id'] = self.latlng_grid['latitude'].astype('str') + '_' + self.latlng_grid['longitude'].astype('str') + '_' + label
            else:
                self.latlng_grid['id'] = self.latlng_grid['latitude'].astype('str') + '_' + self.latlng_grid['longitude'].astype('str')

            self.latlng_grid.to_csv().to_csv('./data/latlng_grid.csv',index=False)
        else:
            pass

    def get_businesses(self,radius=2000,label=None):
        self.headers = {
            'Authorization': f'Bearer {YELP_API_KEY}'
        }
        base_url = 'https://api.yelp.com/v3/businesses/search'
        categories = ['active','arts','auto','beautysvc','bicycles','education',
        'eventservices','financialservices','food','health','homeservices',
        'hotelstravel','localflavor','localservices','massmedia','nightlife',
        'pets','professional','publicservicesgovt','religiousorgs','restaurants','shopping']

        lookup = self.latlng_grid.copy()
        
        for id in tqdm(lookup['id']):
            lat = lookup[lookup['id']==id]['latitude'].values[0]
            lng = lookup[lookup['id']==id]['longitude'].values[0]

            for cat in categories:
                if cat not in self.cache[self.cache['id']==id]['category'].values:
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
                    cache = pd.DataFrame({'id':[id],'category':[cat]})
                    self.new_cache = pd.concat([self.new_cache,cache])
                    
        self.new_bus = self.new_bus.drop_duplicates()

    def save_new_bus(self):
        if (list(self.known_bus.columns) == ['id', 'name', 'url', 'review_count', 'rating', 'price', 'latitude','longitude', 'category'] 
            and list(self.new_bus.columns) == ['id', 'name', 'url', 'review_count', 'rating', 'price', 'latitude','longitude', 'category']
            and list(self.cache.columns) == ['id', 'category']
            and list(self.new_cache.columns) == ['id', 'category']):
            self.known_bus = pd.concat([self.known_bus,self.new_bus]).drop_duplicates().reset_index(drop=True)
            self.known_bus.to_csv('./data/yelp_businesses.csv',index=False)
            self.cache = pd.concat([self.cache,self.new_cache]).drop_duplicates().reset_index(drop=True)
            self.cache.to_csv('./data/yelp_cache.csv',index=False)
            self.new_bus = pd.DataFrame()
        else:
            print('Either known_bus, new_bus, cache or new cache dataframe has incorrect schema.')

