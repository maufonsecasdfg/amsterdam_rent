import pandas as pd
import sys
import os
import requests
import zipfile
import datetime
import geopandas as gpd
import shutil
from shapely.geometry import MultiPolygon, Polygon

# Data from https://www.cbs.nl/nl-nl/maatwerk/2024/35/buurt-wijk-en-gemeente-2024-voor-postcode-huisnummer and https://www.cbs.nl/nl-nl/dossier/nederland-regionaal/geografische-data/wijk-en-buurtkaart-2024

def generate_postcode_gwb_and_geodata_tables(selected_gemeenten, tmp_dir, output_dir, buurt_to_stadsdeel_mapping_path=None):
    ### Using data from: https://www.cbs.nl
    
    # NOTE: A lot of demographic and other data is available in the kaart data. Maybe use this later for something.
    
    current_year = int(datetime.datetime.now().date().strftime("%Y"))
    
    
    pc6_zip_path = os.path.join(tmp_dir, 'postcodes.zip')
    kaart_zip_path = os.path.join(tmp_dir, 'kaart.zip')
    
    pc6_downloaded = False
    kaart_downloaded = False
    for year in [str(current_year), str(current_year-1)]:
        pc6_url = f"https://download.cbs.nl/postcode/{year}-cbs-pc6huisnr{year}0801_buurt.zip"
        kaart_url = f"https://geodata.cbs.nl/files/Wijkenbuurtkaart/WijkBuurtkaart_{year}_v1.zip"

        try:
            # Download the pc6 ZIP file
            print(f"Downloading ZIP file from {pc6_url}...")
            response = requests.get(pc6_url)
            with open(pc6_zip_path, 'wb') as f:
                f.write(response.content)
            print("PC6 download completed.")
            pc6_downloaded = True
            
            # Download the kaart ZIP file
            print(f"Downloading ZIP file from {kaart_url}...")
            response = requests.get(kaart_url)
            with open(kaart_zip_path, 'wb') as f:
                f.write(response.content)
            print("Kaart download completed.")
            kaart_downloaded = True
             
            break
        except:
            print("Data for year {year} not found. Trying for previous year...")
    
    downloaded = pc6_downloaded and kaart_downloaded
    if not downloaded:
        print(f"Data not found. Check code to fix data url. Intended source of data comes from cbs.nl \n PC6 Downloaded: {pc6_downloaded} -> url {pc6_url} \n Kaart Downloaded: {kaart_downloaded} -> url {kaart_url}")
        
    else:
        # Extract the ZIP file
        print("Extracting ZIP files...")
        with zipfile.ZipFile(pc6_zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)
        with zipfile.ZipFile(kaart_zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)
        print("Extraction completed.")        

        # Remove the ZIP file after extraction
        os.remove(pc6_zip_path)
        print(f"ZIP file removed: {pc6_zip_path}")
        os.remove(kaart_zip_path)
        print(f"ZIP file removed: {kaart_zip_path}")
        
        gemeenten_path = os.path.join(tmp_dir, f'gem_{year}.csv')
        wijken_path = os.path.join(tmp_dir, f'wijk_{year}.csv')
        buurten_path = os.path.join(tmp_dir, f'buurt_{year}.csv')
        pc6_path = os.path.join(tmp_dir, f'pc6hnr{year}0801_gwb.csv')
        
        gemeenten_df = pd.read_csv(gemeenten_path, sep=';', encoding='latin1', dtype=str)
        gemeenten_df = gemeenten_df[gemeenten_df['GM_NAAM'].isin(selected_gemeenten)]
        
        pc6_df = pd.read_csv(pc6_path, encoding='latin1', dtype=str)
        pc6_df = (
            pc6_df.rename(columns={'PC6': 'postcode', f'Buurt{year}': 'BU_CODE', f'Wijk{year}': 'WK_CODE', f'Gemeente{year}': 'GM_CODE'})
            .drop(columns=['Huisnummer']).
            drop_duplicates()
        )
        pc6_df['GM_CODE'] = 'GM' + pc6_df['GM_CODE']
        pc6_df['WK_CODE'] = 'WK' + pc6_df['WK_CODE']
        pc6_df['BU_CODE'] = 'BU' + pc6_df['BU_CODE']
        
        gemeente_codes = list(gemeenten_df['GM_CODE'].unique())
        
        pc6_df = pc6_df[pc6_df['GM_CODE'].isin(gemeente_codes)]
        pc6_df['postcode'] = pc6_df['postcode'].str[:4] + ' ' + pc6_df['postcode'].str[4:]
        
        pc6_df = pc6_df.set_index('GM_CODE').join(gemeenten_df.set_index('GM_CODE')).reset_index()
        
        wijken_df = pd.read_csv(wijken_path, sep=';', encoding='latin1', dtype=str)
        
        pc6_df = pc6_df.set_index('WK_CODE').join(wijken_df.set_index('WK_CODE')).reset_index()
        
        buurten_df = pd.read_csv(buurten_path, sep=';', encoding='latin1', dtype=str)
        
        pc6_df = pc6_df.set_index('BU_CODE').join(buurten_df.set_index('BU_CODE')).reset_index()
        
        pc6_df = pc6_df.rename(columns={
            'GM_NAAM': 'gemeente',
            'WK_NAAM': 'wijk',
            'BU_NAAM': 'buurt',
            'GM_CODE': 'gemeente_code',
            'WK_CODE': 'wijk_code',
            'BU_CODE': 'buurt_code'
        })
        
        pc6_df['pc6_jaar'] = year
        
        pc6_df = pc6_df[['postcode', 'gemeente', 'gemeente_code', 'wijk', 'wijk_code', 'buurt', 'buurt_code', 'pc6_jaar']]
        
        if buurt_to_stadsdeel_mapping_path:
            buurt_to_stadsdeel_mapping_df = pd.read_csv(buurt_to_stadsdeel_mapping_path, sep=';')
            pc6_df = pc6_df.set_index('buurt').join(buurt_to_stadsdeel_mapping_df.set_index('buurt')).reset_index()
            pc6_df = pc6_df[['postcode', 'gemeente', 'gemeente_code', 'wijk', 'wijk_code', 'buurt', 'buurt_code', 'stadsdeel_onderverdeling', 'stadsdeel', 'pc6_jaar']]

        pc6_df.to_csv(os.path.join(output_dir, 'postcode_gwb.csv'), sep=';', index=False)
        print(f"Postcode-GWB CSV file saved to: {os.path.join(output_dir, 'postcode_gwb.csv')}")
        
        geodata_path = os.path.join(tmp_dir, f'WijkBuurtkaart_{year}_v1', f'wijkenbuurten_{year}_v1.gpkg')
        
        gemeente_geodata = gpd.read_file(geodata_path,layer='gemeenten')
        gemeente_geodata = gemeente_geodata[(
            (gemeente_geodata['gemeentecode'].isin(gemeente_codes))&
            (gemeente_geodata['water'] != 'JA')
        )][['gemeentecode', 'jaar', 'geometry']]
        gemeente_geodata['geometry_wkt'] = gemeente_geodata['geometry'].apply(lambda geom: geom.wkt)
        gemeente_geodata = (gemeente_geodata
                            .drop(columns=['geometry'])
                            .rename(columns={
                                'gemeentecode': 'gemeente_code',
                                'jaar': 'kaart_jaar',
                                'geometry_wkt': 'geometry_espg28992'
                            }))
        
        gemeente_geodata.to_csv(os.path.join(output_dir, 'gemeente_geodata.csv'), index=False)
        print(f"Gemeente geodata CSV file saved to: {os.path.join(output_dir, 'gemeente_geodata.csv')}")
        
        wijk_geodata = gpd.read_file(geodata_path,layer='wijken')
        wijk_geodata = wijk_geodata[(
            (wijk_geodata['gemeentecode'].isin(gemeente_codes))&
            (wijk_geodata['water'] != 'JA')
        )][['gemeentecode', 'wijkcode', 'jaar', 'geometry']]
        wijk_geodata['geometry_wkt'] = wijk_geodata['geometry'].apply(lambda geom: geom.wkt)
        wijk_geodata = (wijk_geodata
                        .drop(columns=['geometry'])
                        .rename(columns={
                            'gemeentecode': 'gemeente_code',
                            'wijkcode': 'wijk_code',
                            'jaar': 'kaart_jaar',
                            'geometry_wkt': 'geometry_espg28992'
                        }))
        
        wijk_geodata.to_csv(os.path.join(output_dir, 'wijk_geodata.csv'), index=False)
        print(f"Wijk geodata CSV file saved to: {os.path.join(output_dir, 'wijk_geodata.csv')}")
        
        buurt_geodata = gpd.read_file(geodata_path,layer='buurten')
        buurt_geodata = buurt_geodata[(
            (buurt_geodata['gemeentecode'].isin(gemeente_codes))&
            (buurt_geodata['water'] != 'JA')
        )][['gemeentecode', 'wijkcode', 'buurtcode', 'jaar', 'geometry']]
        buurt_geodata['geometry_wkt'] = buurt_geodata['geometry'].apply(lambda geom: geom.wkt)
        buurt_geodata = (buurt_geodata
                        .drop(columns=['geometry'])
                        .rename(columns={
                            'gemmentecode': 'gemeente_code',
                            'wijkcode': 'wijk_code',
                            'buurtcode': 'buurt_code',
                            'jaar': 'kaart_jaar',
                            'geometry_wkt': 'geometry_espg28992',
                        }))

        buurt_geodata.to_csv(os.path.join(output_dir, 'buurt_geodata.csv'), index=False)
        print(f"Buurt geodata CSV file saved to: {os.path.join(output_dir, 'buurt_geodata.csv')}")

    
    return downloaded

def main():
    """
    Usage:
        python script_name.py <city_name_1> <city_name_2> ...
    
    Arguments:
        - <gemeente_name_1>, <gemeente_name_2>, ...: Names of the gemeenten to include in the table.

    Example:
        python script_name.py output.csv Amsterdam Utrecht Leiden
    """
    if len(sys.argv) < 2:
        print("Error: Missing arguments.")
        print("Usage: python script_name.py <gemeente_name_1> <gemeente_name_2> ...")
        print("Provide at least one gemeente name.")
        return

    selected_gemeenten = [gemeente.capitalize() for gemeente in sys.argv[1:]]
    
    print(f"Generating postcode-coordinate table for gemeenten: {', '.join(selected_gemeenten)}")
    print(f"Output will be saved to directory: geodata/")
    
    tmp_dir = "tmp"
    output_dir = "geodata"
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        downloaded = generate_postcode_gwb_and_geodata_tables(
            selected_gemeenten, 
            tmp_dir, 
            output_dir, 
            buurt_to_stadsdeel_mapping_path='geodata/buurt_stadsdeel_mapping.csv'
            )
        if downloaded:
            print("Table generation completed successfully.")
        else:
            pass
    except Exception as e:
        print(f"An error occurred while generating the table: {e}")
        
    # Clean up temporary directory
    shutil.rmtree(tmp_dir)
    print("Temporary directory cleaned up.")

if __name__ == '__main__':
    main()
