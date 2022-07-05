import pandas as pd
import numpy as np
import requests
from tqdm.notebook import tqdm

class CoordScraper():
    def __init__(self):
        self.known_coords = pd.read_csv('./data/postcode_coordinates.csv')
        self.new_coords = pd.DataFrame()
            
    def get_coordinates(self,df):
        coords = {}
        for _, x in tqdm(df.iterrows(),total=df.shape[0]):
            city = x.city.replace('-','+')
            postcode = x.postcode
            if postcode in self.known_coords['postcode'].values:
                continue
            if pd.isna(x.latitude):
                tries = 0
                while tries < 3:
                    url = f'https://geodata.nationaalgeoregister.nl/locatieserver/v3/free?q={city}+{postcode}&fq=type:postcode'
                    result = requests.get(url)
                    if str(result.status_code)[0] == '2':
                        break
                    tries += 1

                if str(result.status_code)[0] != '2':
                    continue

                result = result.json()['response']['docs']
                if len(result) > 0:
                    result = result[0]
                    if 'postcode' in result:
                        if result['postcode'][:4] == postcode[:4]:
                            c = result['centroide_ll'].replace('POINT','').replace('(','').replace(')','').split(' ')
                            lng = c[0]
                            lat = c[1]
                        else:
                            tries = 0
                            while tries < 3:
                                url = f'https://geodata.nationaalgeoregister.nl/locatieserver/v3/free?q={city}+{postcode[:4]}&fq=type:postcode'
                                result = requests.get(url)
                                if result.status_code == 202:
                                    break
                                tries += 1
                            result = result.json()['response']['docs'][0]
                            if len(result) > 0:
                                if 'postcode' in result:
                                    if result['postcode'][:4] == postcode[:4]:
                                        c = result['centroide_ll'].replace('POINT','').replace('(','').replace(')','').split(' ')
                                        lng = c[0]
                                        lat = c[1]
                                    else:
                                        lat = np.nan
                                        lng = np.nan
                    else:
                        lat = np.nan
                        lng = np.nan

                coords[postcode] = {'latitude':float(lat),'longitude':float(lng)}
            else:
                coords[postcode] = {'latitude':float(x.latitude),'longitude':float(x.longitude)}

        coords = pd.DataFrame(coords).T.reset_index().rename(columns={'index':'postcode'})
        self.new_coords = pd.concat([self.new_coords,coords]).drop_duplicates()

    def save_new_coords(self):
        if list(self.known_coords.columns) == ['postcode','latitude','longitude'] and list(self.new_coords.columns) == ['postcode','latitude','longitude']:
            self.known_coords = pd.concat([self.known_coords,self.new_coords]).drop_duplicates().reset_index(drop=True)
            self.known_coords.to_csv('./data/postcode_coordinates.csv',index=False)
            self.new_coords = pd.DataFrame()
        else:
            print('Either known_coords or new_coords dataframe has incorrect schema.')



    