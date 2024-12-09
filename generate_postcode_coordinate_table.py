import json
import pandas as pd
import sys
import os
import requests
import zipfile

def generate_postcode_coordinate_table(output_path, selected_cities):
    ### Using data from: https://github.com/drikusroor/dutch-postal-code-city-coordinates
    
    data_url = "https://github.com/drikusroor/dutch-postal-code-city-coordinates/blob/main/data/postcodetabel-json.zip?raw=true"
    tmp_dir = "tmp"
    
    os.makedirs(tmp_dir, exist_ok=True)
    zip_path = os.path.join(tmp_dir, 'postcodes.zip')

    # Download the ZIP file
    print(f"Downloading ZIP file from {data_url}...")
    response = requests.get(data_url)
    with open(zip_path, 'wb') as f:
        f.write(response.content)
    print("Download completed.")

    # Extract the ZIP file
    print("Extracting ZIP file...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(tmp_dir)
    print("Extraction completed.")

    # Remove the ZIP file after extraction
    os.remove(zip_path)
    print(f"ZIP file removed: {zip_path}")
    
    json_path = os.path.join(tmp_dir, 'postcodetabel.json')

    with open(json_path, 'r') as f:
        postcodes = json.load(f)    

    selected_cities_postcodes = {}
    for pc in postcodes:
        if pc['Plaats'] in selected_cities:
            selected_cities_postcodes[str(pc['PostcodeNummers'])+' '+pc['PostcodeLetters']] = {
                'latitude': pc['Latitude'],
                'longitude': pc['Longitude']
            } 
            
    # Convert to DataFrame and save to CSV
    selected_cities_postcodes_df = (
        pd.DataFrame(selected_cities_postcodes)
        .T.reset_index()
        .rename(columns={'index': 'postcode'})
    )
    selected_cities_postcodes_df.to_csv(output_path, index=False)
    print(f"CSV file saved to: {output_path}")

    selected_cities_postcodes_df.to_csv(output_path, index=False)
    
    # Clean up temporary directory
    for file in os.listdir(tmp_dir):
        os.remove(os.path.join(tmp_dir, file))
    os.rmdir(tmp_dir)
    print("Temporary directory cleaned up.")

def main():
    """
    Usage:
        python script_name.py <output_csv_path> <city_name_1> <city_name_2> ...
    
    Arguments:
        - <output_csv_path>: Path to the output CSV file. The file extension must be .csv.
        - <city_name_1>, <city_name_2>, ...: Names of the cities to include in the table.

    Example:
        python script_name.py output.csv Amsterdam Utrecht Leiden
    """
    if len(sys.argv) < 3:
        print("Error: Missing arguments.")
        print("Usage: python script_name.py <output_csv_path> <city_name_1> <city_name_2> ...")
        print("Provide an output CSV path and at least one city name.")
        return

    output_name = sys.argv[1]
    if not output_name.lower().endswith('.csv'):
        print("Error: The first argument must be a valid CSV file path (e.g., output.csv).")
        return

    selected_cities = [city.capitalize() for city in sys.argv[2:]]
    
    print(f"Generating postcode-coordinate table for cities: {', '.join(selected_cities)}")
    print(f"Output will be saved to: {output_name}")
    
    try:
        generate_postcode_coordinate_table(output_name, selected_cities)
        print("Table generation completed successfully.")
    except Exception as e:
        print(f"An error occurred while generating the table: {e}")

if __name__ == '__main__':
    main()
