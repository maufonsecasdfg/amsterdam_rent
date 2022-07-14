import pandas as pd
import numpy as np
from coordinate_scraper import CoordScraper
import re

def format_postcode(x):
    x = x.split(' ')
    if len(x) > 1:
        x = ''.join(x[:2])
    else:
        x = x[0]
    if len(x) > 6:
        x = x[:4]
    if x[:4].isnumeric():
        return x
    else:
        return None

def format_priceranges(x):
    if ' to ' in x:
        x = x.split('to')
        n1 = float(re.sub("[^\\d.]","",x[0]))
        n2 = float(re.sub("[^\\d.]","",x[1]))
        return str((n1+n2)/2)
    if ' - ' in x:
        x = x.split('-')
        n1 = float(re.sub("[^\\d.]","",x[0]))
        n2 = float(re.sub("[^\\d.]","",x[1]))
        return str((n1+n2)/2)
    else:
        return x

def format_surfranges(x):
    if ' - ' in x:
        x = x.split(' - ')
        n1 = float(re.sub("[^\\d.]","",x[0]))
        n2 = float(re.sub("[^\\d.]","",x[1]))
        return str((n1+n2)/2)
    else:
        return x

def impute_rooms(room_type_medians,x):
    if pd.isna(x.rooms):
        return room_type_medians[x.type]
    else:
        return x.rooms

class DataFormatter():
    def __init__(self,scraped_filename):
        self.data = self.prepare_raw_data(scraped_filename)
        self.train_data, self.val_data = self.train_val_split()
        self.room_type_medians = dict()
        self.trained = False

    def run_formatting(self,train):
        if train:
            self.format_data(self.train_data,min_price=100,train=True)
        else:
            self.format_data(self.val_data,min_price=100,train=True)
    
    def train_val_split(self):
        #TODO
        return self.data, pd.DataFrame(columns=self.data.columns)

    def generate_room_type_medians(self):
        self.room_type_medians = self.train_data.groupby('type')['rooms'].median()

    def prepare_raw_data(self,scraped_filename):
        try:
            scraped = pd.read_csv(f'./data/{scraped_filename}')
        except FileNotFoundError as e:
            print('Scraped file does not exist.')
        rentkagg = pd.read_json('./data/properties.json',lines=True)
        buykagg = pd.read_csv('./data/HousingPrices-Amsterdam-August-2021.csv')
        
        cities = ['amsterdam', 'amstelveen', 'diemen', 'zaandam', 'haarlem', 'weesp', 'ouderkerk-aan-de-amstel',
            'ouder-amstel','uithoorn','aalsmeer','haarlemmermeer',
            'duivendrecht','hoofddorp','nieuw-vennep','badhoevedorp',
            'ruigoord','zwanenburg','lijnen','de-liede']

        rentkagg = rentkagg[['areaSqm','city','furnish','latitude','longitude','postalCode','propertyType','rent','source','matchCapacity']]
        rentkagg = rentkagg[rentkagg.city.str.lower().isin(cities)]
        rentkagg = rentkagg.rename(columns={
                'areaSqm' : 'surface',
                'rent': 'price',
                'furnish': 'furnished',
                'postalCode': 'postcode',
                'propertyType': 'type',
                'matchCapacity': 'rooms'
            })
        rentkagg['postType'] = 'Rent'

        buykagg = buykagg.rename(columns={
            'Area' : 'surface',
            'Price': 'price',
            'Zip': 'postcode',
            'Room': 'rooms',
            'Lon': 'longitude',
            'Lat': 'latitude'
        })
        buykagg['source'] = 'Pararius'
        buykagg['type'] = 'house'
        buykagg['postType'] = 'Buy'
        buykagg['city'] = 'amsterdam'
        buykagg['furnished'] = None
        buykagg.drop(columns=['Unnamed: 0','Address'],inplace=True)

        data = pd.concat([scraped,rentkagg,buykagg]).reset_index(drop=True).drop_duplicates()
        return data

    def format_data(self,data,min_price=100,train=True):
        if not train and not self.trained:
            print('Train data not yet formatted. Aborting.')
            return None

        data['type'] = data['type'].str.replace(' - ','').str.strip().str.lower()
        data['type'] = data['type'].replace('anti-squat','apartment')
        data['type'] = data['type'].replace('student residence','apartment')
        data['type'] = data['type'].replace('appartement','apartment')
        data['type'] = data['type'].replace('appartment','apartment')
        data['type'] = data['type'].replace('project:','apartment')
        data['type'] = data['type'].replace('huis','house')

        data['rooms'] = data['rooms'].replace('Not important',None)
        data['rooms'] = data['rooms'].str.replace("[^\\d]","",regex=True).astype('float')

        if train:
            self.generate_room_type_medians()

        data['rooms'] = data.apply(lambda x: impute_rooms(self.room_type_medians,x),axis=1)

        data['postcode'] = data['postcode'].apply(format_postcode)

        data['price'] = data['price'].astype('str').str.replace('.0','',regex=False).str.replace(',-','',regex=False).str.replace('.','',regex=False).str.replace('k.k.','',regex=False)
        data['price'] = data['price'].apply(format_priceranges).str.replace("[^\\d.]","",regex=True).replace('',None).astype('float')

        data = data.dropna(subset=['price','postcode'])

        data = data[data['price'] > min_price].reset_index(drop=True)

        data['surface'] = data['surface'].astype('str').apply(format_surfranges).str.replace(',','').str.replace('m2','').str.replace('m²','').str.strip().astype(float)

        data = data.dropna(subset=['surface','price'])

        data['source'] = data['source'].str.lower()
        data['city'] = data['city'].str.lower()

        data = data.drop(columns=['furnished'])

        coordscraper = CoordScraper()
        data['latitude'] = np.nan
        data['longitude'] = np.nan

        coordmap = coordscraper.known_coords.copy()
        coordmap.set_index('postcode',inplace=True)

        data.loc[data.latitude.isna(),'latitude'] = data[data.latitude.isna()].postcode.map(dict(coordmap.latitude))
        data.loc[data.longitude.isna(),'longitude'] = data[data.longitude.isna()].postcode.map(dict(coordmap.longitude))

        print('Finding Coordinated for missing Postcodes')
        coordscraper.get_coordinates(data[data['latitude'].isna()][['postcode','city','latitude','longitude']].drop_duplicates())
        coordscraper.save_new_coords()

        coordmap = coordscraper.known_coords.copy()
        coordmap.set_index('postcode',inplace=True)
        data.loc[data.latitude.isna(),'latitude'] = data[data.latitude.isna()].postcode.map(dict(coordmap.latitude))
        data.loc[data.longitude.isna(),'longitude'] = data[data.longitude.isna()].postcode.map(dict(coordmap.longitude))

        if train:
            self.trained = True

        return data
