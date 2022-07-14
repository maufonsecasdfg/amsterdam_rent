import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon

class CityGeometries():

    def __init__(self):
        data = pd.read_csv('./data/formatted_data.csv')
        self.gdata = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.longitude, data.latitude))
        self.gdata = self.gdata.set_crs('EPSG:4326')

        self.raw_buurten = gpd.read_file('./data/WijkBuurtkaart_2021_v1.gpkg',layer='cbs_buurten_2021')
        wijken = gpd.read_file('./data/WijkBuurtkaart_2021_v1.gpkg',layer='cbs_wijken_2021')
        gemeenten = gpd.read_file('./data/WijkBuurtkaart_2021_v1.gpkg',layer='gemeenten2021')
        self.raw_buurten = self.raw_buurten.set_index(['gemeentecode','wijkcode']).join(wijken.set_index(['gemeentecode','wijkcode'])['wijknaam']).reset_index()

        self.raw_buurten = self.raw_buurten[['gemeentenaam','wijknaam','buurtnaam','geometry']].copy()
        self.raw_buurten['buurtnaam'] = self.raw_buurten['buurtnaam'].str.strip().replace('',np.nan).dropna()
        self.raw_buurten = self.raw_buurten.to_crs('EPSG:4326')

        self.gdata = gpd.sjoin(self.gdata, self.raw_buurten, predicate='within').drop(columns='index_right')

        self.buurten = pd.DataFrame()
        self.props = pd.DataFrame()
        self.geometry = pd.DataFrame()

    def compute_count(self):
        count = (self.props.groupby(['gemeentenaam','wijknaam','buurtnaam']).count()[['source']]
                    .reset_index().set_index(['gemeentenaam','wijknaam']).join(
                    self.props.groupby(['gemeentenaam','wijknaam']).count()[['geometry']]))
        count = count.rename(columns={
                            'source': 'buurt_count',
                            'geometry': 'wijk_count'})
        count = count.reset_index()
        return count

    def populate_geometry(self,count_threshold=30):
        self.props = self.gdata.copy()
        self.buurten = self.raw_buurten.copy()

        no_useful_neighs = pd.DataFrame()
        print('INTRAWIJK')
        while True:
            count = self.compute_count()

            few_props = count[(count['buurt_count']<count_threshold)&(count['wijk_count']>=count_threshold)]
            if len(few_props) != 0:
                row = few_props.iloc[0]
                gem = row['gemeentenaam']
                wik = row['wijknaam']
                brt = row['buurtnaam']
                same_wijk_count = count[(count['gemeentenaam']==gem)&(count['wijknaam']==wik)]
                
                gdf1 = self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik)&(self.buurten['buurtnaam']==brt)].copy()
                gdf1['geometry'] = gdf1['geometry'].boundary
                gdf2 = self.buurten[(self.buurten['gemeentenaam']== gem)&(self.buurten['wijknaam']==wik)].copy()
                gdf2['geometry'] = gdf2['geometry'].boundary
                
                neighbuurts = gpd.sjoin(gdf1, 
                                        gdf2, predicate='overlaps')[['gemeentenaam_left','wijknaam_left','buurtnaam_right']]
                neighbuurts.columns = ['gemeentenaam','wijknaam','buurtnaam']
                neighbuurts = neighbuurts[neighbuurts['buurtnaam']!=brt]
                indexes = neighbuurts.set_index(['gemeentenaam','wijknaam','buurtnaam']).join(same_wijk_count.set_index(['gemeentenaam','wijknaam','buurtnaam']))['buurt_count'].sort_values().index
                if len(indexes) != 0:
                    brt_tojoin = indexes[0][2]
                else:
                    print(f'Failed to find neighbour buurt for {brt}')
                    no_useful_neighs =  pd.concat([no_useful_neighs,self.props[(self.props['gemeentenaam']==gem)&
                                                                        (self.props['wijknaam']==wik)&
                                                                        (self.props['buurtnaam']==brt)]])
                    self.props = self.props[~((self.props['gemeentenaam']==gem)&
                                (self.props['wijknaam']==wik)&
                                (self.props['buurtnaam']==brt))].reset_index(drop=True)
                    continue 
                    
                jburt = brt_tojoin+' & '+brt

                print(jburt)
                
                try:
                
                    united_geometry = MultiPolygon([self.buurten[(self.buurten['gemeentenaam']==gem)&
                                    (self.buurten['wijknaam']==wik)&
                                    (self.buurten['buurtnaam']==brt)]['geometry'].values[0].union(self.buurten[(self.buurten['gemeentenaam']==gem)&
                                    (self.buurten['wijknaam']==wik)&
                                    (self.buurten['buurtnaam']==brt_tojoin)]['geometry'].values[0])])

                

                except:
                    h1 = self.buurten[(self.buurten['gemeentenaam']==gem)&
                                (self.buurten['wijknaam']==wik)&
                                (self.buurten['buurtnaam']==brt)]['geometry'].values
                    h2 = self.buurten[(self.buurten['gemeentenaam']==gem)&
                                (self.buurten['wijknaam']==wik)&
                                (self.buurten['buurtnaam']==brt_tojoin)]['geometry'].values
                    seq1 = []
                    for pol in h1:
                        seq1.extend([i for i in pol.geoms])
                    mp1 = MultiPolygon(seq1)
                    seq2 = []
                    for pol in h2:
                        seq2.extend([i for i in pol.geoms])
                    mp2 = MultiPolygon(seq2)
                    united_geometry = mp1.union(mp2)
            

                union_data = {'gemeentenaam':[gem],
                        'wijknaam':[wik],
                        'buurtnaam':[jburt],
                        'geometry':[united_geometry]}

                self.buurten = self.buurten.drop(index=self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik)&(self.buurten['buurtnaam']==brt)].index[0]).reset_index(drop=True)
                self.buurten = self.buurten.drop(index=self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik)&(self.buurten['buurtnaam']==brt_tojoin)].index[0]).reset_index(drop=True)

                self.buurten = gpd.GeoDataFrame(pd.concat([self.buurten,gpd.GeoDataFrame(union_data)]).reset_index(drop=True))
                #print(type(self.buurten))
                
                self.props.reset_index(drop=True,inplace=True)
                
                self.props.loc[(self.props['gemeentenaam']==gem)&
                        (self.props['wijknaam']==wik)&
                        (self.props['buurtnaam']==brt), 'buurtnaam'] = jburt
                self.props.loc[(self.props['gemeentenaam']==gem)&
                        (self.props['wijknaam']==wik)&
                        (self.props['buurtnaam']==brt_tojoin), 'buurtnaam'] = jburt
                
            else:
                break
                
        self.props = pd.concat([self.props,no_useful_neighs])
        no_useful_neighs = pd.DataFrame()

        print('----')
        print('INTERWIJK')
        while True:
            count = self.compute_count()

            few_props = count[(count['buurt_count']<count_threshold)]
            if len(few_props) != 0:
                row = few_props.iloc[0]
                gem = row['gemeentenaam']
                wik = row['wijknaam']
                brt = row['buurtnaam']
                
                gdf1 = self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik)&(self.buurten['buurtnaam']==brt)].copy()
                gdf1['geometry'] = gdf1['geometry'].boundary
                gdf2 = self.buurten[(self.buurten['gemeentenaam']== gem)].copy()
                gdf2['geometry'] = gdf2['geometry'].boundary
                
                neighbuurts = gpd.sjoin(gdf1, gdf2, predicate='overlaps')[['gemeentenaam_left','wijknaam_right','buurtnaam_right']]
                
                neighbuurts.columns = ['gemeentenaam','wijknaam','buurtnaam']
                neighbuurts = neighbuurts[neighbuurts['buurtnaam']!=brt]
                
                indexes = neighbuurts.set_index(['gemeentenaam','wijknaam','buurtnaam']).join(count.set_index(['gemeentenaam','wijknaam','buurtnaam']))['buurt_count'].sort_values().index
                if len(indexes) != 0:
                    wik_tojoin = indexes[0][1]
                    brt_tojoin = indexes[0][2]
                else:
                    print(f'Failed to find neighbour buurt for {brt}')
                    no_useful_neighs =  pd.concat([no_useful_neighs,self.props[(self.props['gemeentenaam']==gem)&
                                                                        (self.props['wijknaam']==wik)&
                                                                        (self.props['buurtnaam']==brt)]])
                    self.props = self.props[~((self.props['gemeentenaam']==gem)&
                                (self.props['wijknaam']==wik)&
                                (self.props['buurtnaam']==brt))].reset_index(drop=True)
                    continue 
                    
                jburt = brt_tojoin+' & '+brt
                jwik = wik_tojoin+' & '+wik

                print(jburt)
                
                try:
                
                    united_geometry = MultiPolygon([self.buurten[(self.buurten['gemeentenaam']==gem)&
                                    (self.buurten['wijknaam']==wik)&
                                    (self.buurten['buurtnaam']==brt)]['geometry'].values[0].union(self.buurten[(self.buurten['gemeentenaam']==gem)&
                                    (self.buurten['wijknaam']==wik)&
                                    (self.buurten['buurtnaam']==brt_tojoin)]['geometry'].values[0])])

                

                except:
                    h1 = self.buurten[(self.buurten['gemeentenaam']==gem)&
                                (self.buurten['wijknaam']==wik)&
                                (self.buurten['buurtnaam']==brt)]['geometry'].values
                    h2 = self.buurten[(self.buurten['gemeentenaam']==gem)&
                                (self.buurten['wijknaam']==wik)&
                                (self.buurten['buurtnaam']==brt_tojoin)]['geometry'].values
                    seq1 = []
                    for pol in h1:
                        seq1.extend([i for i in pol.geoms])
                    mp1 = MultiPolygon(seq1)
                    seq2 = []
                    for pol in h2:
                        seq2.extend([i for i in pol.geoms])
                    mp2 = MultiPolygon(seq2)
                    united_geometry = mp1.union(mp2)

                jwik = jwik.split(' & ')
                jwik = ' & '.join(list(set(jwik)))

                union_data = {'gemeentenaam':[gem],
                        'wijknaam':[jwik],
                        'buurtnaam':[jburt],
                        'geometry':[united_geometry]}

                self.buurten = self.buurten.drop(index=self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik)&(self.buurten['buurtnaam']==brt)].index[0]).reset_index(drop=True)
                self.buurten = self.buurten.drop(index=self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik_tojoin)&(self.buurten['buurtnaam']==brt_tojoin)].index[0]).reset_index(drop=True)
                
                self.buurten = gpd.GeoDataFrame(pd.concat([self.buurten,gpd.GeoDataFrame(union_data)]).reset_index(drop=True))

                
                self.props.loc[(self.props['gemeentenaam']==gem)&
                        (self.props['wijknaam']==wik)&
                        (self.props['buurtnaam']==brt), 'buurtnaam'] = jburt
                self.props.loc[(self.props['gemeentenaam']==gem)&
                        (self.props['wijknaam']==wik)&
                        (self.props['buurtnaam']==jburt), 'wijknaam'] = jwik
                
                self.props.loc[(self.props['gemeentenaam']==gem)&
                        (self.props['wijknaam']==wik_tojoin)&
                        (self.props['buurtnaam']==brt_tojoin), 'buurtnaam'] = jburt
                self.props.loc[(self.props['gemeentenaam']==gem)&
                        (self.props['wijknaam']==wik_tojoin)&
                        (self.props['buurtnaam']==jburt), 'wijknaam'] = jwik
            else:
                break
                
        self.props = pd.concat([self.props,no_useful_neighs])
        no_useful_neighs = pd.DataFrame()
        
        print('----')
        print('INTEGEEM')
        while True:
            count = self.compute_count()

            few_props = count[(count['buurt_count']<count_threshold)]
            if len(few_props) != 0:
                row = few_props.iloc[0]
                gem = row['gemeentenaam']
                wik = row['wijknaam']
                brt = row['buurtnaam']
                
                gdf1 = self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik)&(self.buurten['buurtnaam']==brt)].copy()
                gdf1['geometry'] = gdf1['geometry'].boundary
                gdf2 = self.buurten.copy()
                gdf2['geometry'] = gdf2['geometry'].boundary
                
                neighbuurts = gpd.sjoin(gdf1, gdf2, predicate='overlaps')[['gemeentenaam_right','wijknaam_right','buurtnaam_right']]
                
                neighbuurts.columns = ['gemeentenaam','wijknaam','buurtnaam']
                neighbuurts = neighbuurts[neighbuurts['buurtnaam']!=brt]
                
                indexes = neighbuurts.set_index(['gemeentenaam','wijknaam','buurtnaam']).join(count.set_index(['gemeentenaam','wijknaam','buurtnaam']))['buurt_count'].sort_values().index
                if len(indexes) != 0:
                    gem_tojoin = indexes[0][0]
                    wik_tojoin = indexes[0][1]
                    brt_tojoin = indexes[0][2]
                else:
                    print(f'Failed to find neighbour buurt for {brt}')
                    no_useful_neighs =  pd.concat([no_useful_neighs,self.props[(self.props['gemeentenaam']==gem)&
                                                                        (self.props['wijknaam']==wik)&
                                                                        (self.props['buurtnaam']==brt)]])
                    self.props = self.props[~((self.props['gemeentenaam']==gem)&
                                (self.props['wijknaam']==wik)&
                                (self.props['buurtnaam']==brt))].reset_index(drop=True)
                    continue 
                    
                jgem = gem_tojoin+' & '+gem
                jburt = brt_tojoin+' & '+brt
                jwik = wik_tojoin+' & '+wik

                print(jburt)
                
                try:
                    united_geometry = MultiPolygon([self.buurten[(self.buurten['gemeentenaam']==gem)&
                                    (self.buurten['wijknaam']==wik)&
                                    (self.buurten['buurtnaam']==brt)]['geometry'].values[0].union(self.buurten[(self.buurten['gemeentenaam']==gem)&
                                    (self.buurten['wijknaam']==wik)&
                                    (self.buurten['buurtnaam']==brt_tojoin)]['geometry'].values[0])])

                except:
                    h1 = self.buurten[(self.buurten['gemeentenaam']==gem)&
                                (self.buurten['wijknaam']==wik)&
                                (self.buurten['buurtnaam']==brt)]['geometry'].values
                    h2 = self.buurten[(self.buurten['gemeentenaam']==gem)&
                                (self.buurten['wijknaam']==wik)&
                                (self.buurten['buurtnaam']==brt_tojoin)]['geometry'].values
                    seq1 = []
                    for pol in h1:
                        seq1.extend([i for i in pol.geoms])
                    mp1 = MultiPolygon(seq1)
                    seq2 = []
                    for pol in h2:
                        seq2.extend([i for i in pol.geoms])
                    mp2 = MultiPolygon(seq2)
                    united_geometry = mp1.union(mp2)
                    
                jwik = jwik.split(' & ')
                jwik = ' & '.join(list(set(jwik)))

                jgem = jgem.split(' & ')
                jgem = ' & '.join(list(set(jgem)))

                union_data = {'gemeentenaam':[jgem],
                        'wijknaam':[jwik],
                        'buurtnaam':[jburt],
                        'geometry':[united_geometry]}

                self.buurten = self.buurten.drop(index=self.buurten[(self.buurten['gemeentenaam']==gem)&(self.buurten['wijknaam']==wik)&(self.buurten['buurtnaam']==brt)].index[0]).reset_index(drop=True)
                self.buurten = self.buurten.drop(index=self.buurten[(self.buurten['gemeentenaam']==gem_tojoin)&(self.buurten['wijknaam']==wik_tojoin)&(self.buurten['buurtnaam']==brt_tojoin)].index[0]).reset_index(drop=True)

                self.buurten = gpd.GeoDataFrame(pd.concat([self.buurten,gpd.GeoDataFrame(union_data)]).reset_index(drop=True))
                
                self.props.loc[(self.props['gemeentenaam']==gem)&
                        (self.props['wijknaam']==wik)&
                        (self.props['buurtnaam']==brt), 'gemeentenaam'] = jgem
                self.props.loc[(self.props['gemeentenaam']==jgem)&
                        (self.props['wijknaam']==wik)&
                        (self.props['buurtnaam']==brt), 'buurtnaam'] = jburt
                self.props.loc[(self.props['gemeentenaam']==jgem)&
                        (self.props['wijknaam']==wik)&
                        (self.props['buurtnaam']==jburt), 'wijknaam'] = jwik
                
                self.props.loc[(self.props['gemeentenaam']==gem_tojoin)&
                        (self.props['wijknaam']==wik_tojoin)&
                        (self.props['buurtnaam']==brt_tojoin), 'gemeentenaam'] = jgem
                self.props.loc[(self.props['gemeentenaam']==jgem)&
                        (self.props['wijknaam']==wik_tojoin)&
                        (self.props['buurtnaam']==brt_tojoin), 'buurtnaam'] = jburt
                self.props.loc[(self.props['gemeentenaam']==jgem)&
                        (self.props['wijknaam']==wik_tojoin)&
                        (self.props['buurtnaam']==jburt), 'wijknaam'] = jwik
            else:
                break
        
        print(f'Number of properties not appendable to region big enough: {len(no_useful_neighs)}')
        self.props = pd.concat([self.props,no_useful_neighs])
        count = self.compute_count()
        self.geometry = count.set_index(['gemeentenaam','wijknaam','buurtnaam'])[[]].join(self.buurten.set_index(['gemeentenaam','wijknaam','buurtnaam'])).reset_index()
        self.geometry = gpd.GeoDataFrame(self.geometry)

    def save_geometries(self):
        self.props.to_csv('./data/localized_data.csv',index=False)
        self.geometry.to_csv('./data/citygeometry.csv',index=False)
