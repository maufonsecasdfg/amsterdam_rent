import pandas as pd
import sys
import os
import requests
import zipfile
import datetime


# Data from https://www.cbs.nl/nl-nl/maatwerk/2024/35/buurt-wijk-en-gemeente-2024-voor-postcode-huisnummer

def generate_postcode_gwb_table(output_path, selected_gemeenten):
    ### Using data from: https://www.cbs.nl
    
    current_year = int(datetime.datetime.now().date().strftime("%Y"))
    
    tmp_dir = "tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    
    zip_path = os.path.join(tmp_dir, 'postcodes.zip')
    
    downloaded = True
    for year in [str(current_year), str(current_year-1)]:
        data_url = f"https://download.cbs.nl/postcode/{year}-cbs-pc6huisnr{year}0801_buurt.zip"

        try:
            # Download the ZIP file
            print(f"Downloading ZIP file from {data_url}...")
            response = requests.get(data_url)
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            downloaded = True
            print("Download completed.")
            break
        except:
            print("Data for year {year} not found. Trying for previous year...")
    
    if not downloaded:
        print("Data not found. Check code to fix data url. Intended source of data comes from cbs.nl")
        
    else:
        # Extract the ZIP file
        print("Extracting ZIP file...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)
        print("Extraction completed.")

        # Remove the ZIP file after extraction
        os.remove(zip_path)
        print(f"ZIP file removed: {zip_path}")
        
        gemeenten_path = os.path.join(tmp_dir, f'gem_{year}.csv')
        wijken_path = os.path.join(tmp_dir, f'wijk_{year}.csv')
        buurten_path = os.path.join(tmp_dir, f'buurt_{year}.csv')
        pc6_path = os.path.join(tmp_dir, f'pc6hnr{year}0801_gwb.csv')
        
        gemeenten_df = pd.read_csv(gemeenten_path, sep=';', encoding='latin1', dtype=str)
        gemeenten_df = gemeenten_df[gemeenten_df['GM_NAAM'].isin(selected_gemeenten)]
        gemeenten_df['GM_CODE'] = gemeenten_df['GM_CODE'].str.replace('GM', '', regex=False)
        
        pc6_df = pd.read_csv(pc6_path, encoding='latin1', dtype=str)
        pc6_df = (
            pc6_df.rename(columns={'PC6': 'postcode', f'Buurt{year}': 'BU_CODE', f'Wijk{year}': 'WK_CODE', f'Gemeente{year}': 'GM_CODE'})
            .drop(columns=['Huisnummer']).
            drop_duplicates()
        )
        pc6_df = pc6_df[pc6_df['GM_CODE'].isin(gemeenten_df['GM_CODE'])]
        pc6_df['postcode'] = pc6_df['postcode'].str[:4] + ' ' + pc6_df['postcode'].str[4:]
        
        pc6_df = pc6_df.set_index('GM_CODE').join(gemeenten_df.set_index('GM_CODE')).reset_index()
        
        wijken_df = pd.read_csv(wijken_path, sep=';', encoding='latin1', dtype=str)
        wijken_df['WK_CODE'] = wijken_df['WK_CODE'].str.replace('WK', '', regex=False)
        
        pc6_df = pc6_df.set_index('WK_CODE').join(wijken_df.set_index('WK_CODE')).reset_index()
        
        buurten_df = pd.read_csv(buurten_path, sep=';', encoding='latin1', dtype=str)
        buurten_df['BU_CODE'] = buurten_df['BU_CODE'].str.replace('BU', '', regex=False)
        
        pc6_df = pc6_df.set_index('BU_CODE').join(buurten_df.set_index('BU_CODE')).reset_index()
        
        pc6_df = pc6_df.rename(columns={
            'GM_NAAM': 'gemeente',
            'WK_NAAM': 'wijk',
            'BU_NAAM': 'buurt',
            'GM_CODE': 'gemeente_code',
            'WK_CODE': 'wijk_code',
            'BU_CODE': 'buurt_code'
        })
        
        pc6_df = pc6_df[['postcode', 'gemeente', 'gemeente_code', 'wijk', 'wijk_code', 'buurt', 'buurt_code']]

        pc6_df.to_csv(output_path, sep=';', index=False)
        print(f"CSV file saved to: {output_path}")

    
    # Clean up temporary directory
    for file in os.listdir(tmp_dir):
        os.remove(os.path.join(tmp_dir, file))
    os.rmdir(tmp_dir)
    print("Temporary directory cleaned up.")
    
    return downloaded

def main():
    """
    Usage:
        python script_name.py <output_csv_path> <city_name_1> <city_name_2> ...
    
    Arguments:
        - <output_csv_path>: Path to the output CSV file. The file extension must be .csv.
        - <gemeente_name_1>, <gemeente_name_2>, ...: Names of the gemeenten to include in the table.

    Example:
        python script_name.py output.csv Amsterdam Utrecht Leiden
    """
    if len(sys.argv) < 3:
        print("Error: Missing arguments.")
        print("Usage: python script_name.py <output_csv_path> <gemeente_name_1> <gemeente_name_2> ...")
        print("Provide an output CSV path and at least one gemeente name.")
        return

    output_name = sys.argv[1]
    if not output_name.lower().endswith('.csv'):
        print("Error: The first argument must be a valid CSV file path (e.g., output.csv).")
        return

    selected_gemeenten = [gemeente.capitalize() for gemeente in sys.argv[2:]]
    
    print(f"Generating postcode-coordinate table for gemeenten: {', '.join(selected_gemeenten)}")
    print(f"Output will be saved to: {output_name}")
    
    try:
        downloaded = generate_postcode_gwb_table(output_name, selected_gemeenten)
        if downloaded:
            print("Table generation completed successfully.")
        else:
            pass
    except Exception as e:
        print(f"An error occurred while generating the table: {e}")

if __name__ == '__main__':
    main()
