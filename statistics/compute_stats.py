from google.cloud import bigquery
import pandas as pd
import numpy as np
import json

def fetch_data(bigquery_config):
    client = bigquery.Client()

    query = f"""
    SELECT 
    p.*,
    q.*
    FROM `{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property` p
    LEFT JOIN `{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.postcode_gwb` q
    ON p.postcode = q.postcode
    """

    query_job = client.query(query)
    results = query_job.result() 

    dataframe = results.to_dataframe()

    return dataframe

def generate_stadsdeel_subdivision_map(df):
    subdivision_stadsdeel_map = df[['stadsdeel','stadsdeel_onderverdeling']].drop_duplicates().set_index('stadsdeel_onderverdeling').to_dict('index')
    subdivision_stadsdeel_map = {s : subdivision_stadsdeel_map[s]['stadsdeel'] for s in subdivision_stadsdeel_map}
    return subdivision_stadsdeel_map

def process_data(df):
    df = df.copy()
    df = df.drop_duplicates()
    df = df[df['stadsdeel']!='Weesp']
    df['price_per_m2'] = df['price']/df['surface']
    df['price_per_room'] = df['price']/df['rooms']
    
    property_type_map = {
        'Appartement': 'Apartment',
        'Flat': 'Apartment',
        'Studio': 'Apartment',
        'House': 'House',
        'Huis': 'House',
        'Room': 'Room'
    }
    df['property_type'] = df['property_type'].map(property_type_map)
    
    furnished_map = {
        'Upholstered': 'Upholstered',
        'Furnished': 'Furnished',
        'Upholstered or furnished': 'Furnished',
        'Shell': 'Shell'
    }
    df.loc[df['post_type']=='Rent', 'furnished'] = df.loc[df['post_type']=='Rent', 'furnished'].map(furnished_map)
    
    return df
    
def remove_outliers(df, column_name, percentile_bounds):
    processed_df = df.copy()
    lower_bound = processed_df[column_name].quantile(percentile_bounds[0])
    upper_bound = processed_df[column_name].quantile(percentile_bounds[1])
    processed_df = processed_df[(
        (processed_df[column_name] >= lower_bound)&
        (processed_df[column_name] <= upper_bound)
    )]
    return processed_df

def compute_statistics(df, column_name, post_type, property_type, furnished, region_resolution, 
                       percentile_bounds, subdivision_stadsdeel_map, min_properties_to_compute_stats):
    stats = []
    df_s = df.copy()
    df_s[f'log_{column_name}'] = np.log(df_s[column_name])
    df_s = df_s[df_s['post_type']==post_type]
    if property_type != 'All':
        df_s = df_s[df_s['property_type'] == property_type]
    if post_type == 'Rent':
        if furnished != 'All':
            df_s = df_s[df_s['furnished']==furnished]
    for region in df[region_resolution].unique():
        if region_resolution == 'stadsdeel':
            stadsdeel = region
            subdivision = None
        elif region_resolution == 'stadsdeel_onderverdeling':
            stadsdeel = subdivision_stadsdeel_map[region]
            subdivision = region
        df_r = df_s[df_s[region_resolution]==region]
        property_count = len(df_r)
        
        processed_df = remove_outliers(df_r, f'log_{column_name}', percentile_bounds)
        
        if len(processed_df) < min_properties_to_compute_stats:
            stats.append({
            'region_resolution': region_resolution,
            'stadsdeel': stadsdeel,
            'subdivision': subdivision,
            'post_type': post_type,
            'property_type': property_type,
            'furnished': furnished,
            'value': column_name,
            'number_of_properties': property_count
        })
        
        median = processed_df[column_name].median()
        q1 = processed_df[column_name].quantile(0.25)
        q3 = processed_df[column_name].quantile(0.75)
        mode = processed_df[column_name].mode()
        if len(mode) == 1:
            mode = mode.iloc[0]
        else:
            mode = None
        log_mean = processed_df[f'log_{column_name}'].mean()
        log_std = processed_df[f'log_{column_name}'].std()
        geometric_mean = np.exp(log_mean)
        geometric_std = np.exp(log_std)
        geometric_conf_int_95_low = np.exp(log_mean-1.96*log_std)
        geometric_conf_int_95_upp = np.exp(log_mean+1.96*log_std)
        geometric_conf_int_75_low = np.exp(log_mean-1.15*log_std)
        geometric_conf_int_75_upp = np.exp(log_mean+1.15*log_std)
        geometric_conf_int_50_low = np.exp(log_mean-0.674*log_std)
        geometric_conf_int_50_upp = np.exp(log_mean+0.674*log_std)
        stats.append({
            'region_resolution': region_resolution,
            'stadsdeel': stadsdeel,
            'subdivision': subdivision,
            'post_type': post_type,
            'property_type': property_type,
            'furnished': furnished,
            'value': column_name,
            'median': median,
            'q1': q1,
            'q3': q3,
            'mode': mode,
            'geometric_mean': geometric_mean,
            'geometric_std': geometric_std,
            'geometric_conf_int_95_low': geometric_conf_int_95_low,
            'geometric_conf_int_95_upp': geometric_conf_int_95_upp,
            'geometric_conf_int_75_low': geometric_conf_int_75_low,
            'geometric_conf_int_75_upp': geometric_conf_int_75_upp,
            'geometric_conf_int_50_low': geometric_conf_int_50_low,
            'geometric_conf_int_50_upp': geometric_conf_int_50_upp,
            'number_of_properties': property_count
        })
    stats_df = pd.DataFrame(stats)
    return stats_df

def run_stats_computation(df, percentile_bounds, subdivision_stadsdeel_map, min_properties_to_compute_stats):
    stats_df = pd.DataFrame()
    for region_resolution in ['stadsdeel', 'stadsdeel_onderverdeling']:
        for post_type in ['Buy', 'Rent']:
            for property_type in ['All', 'Apartment', 'House']:
                for furnished in [None, 'All', 'Upholstered', 'Furnished', 'Shell']:
                    if post_type == 'Buy' and furnished is not None:
                        continue
                    elif post_type == 'Rent' and furnished is None:
                        continue
                    for column_name in ['price', 'surface', 'rooms', 'price_per_m2', 'price_per_room']:
                        s = compute_statistics(
                                df,  
                                column_name=column_name,
                                post_type=post_type,
                                property_type=property_type,
                                furnished = furnished,
                                region_resolution=region_resolution,
                                percentile_bounds=percentile_bounds,
                                subdivision_stadsdeel_map=subdivision_stadsdeel_map,
                                min_properties_to_compute_stats=min_properties_to_compute_stats
                            )
                        stats_df = pd.concat([stats_df,s])
    return stats_df

def upload_dataframe_to_bigquery(df, bigquery_config):
    table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.stats"
    client = bigquery.Client()

    schema = [
        bigquery.SchemaField("region_resolution", "STRING"),
        bigquery.SchemaField("stadsdeel", "STRING"),
        bigquery.SchemaField("subdivision", "STRING"),
        bigquery.SchemaField("post_type", "STRING"),
        bigquery.SchemaField("property_type", "STRING"),
        bigquery.SchemaField("furnished", "STRING"),
        bigquery.SchemaField("value", "STRING"),
        bigquery.SchemaField("median", "FLOAT"),
        bigquery.SchemaField("q1", "FLOAT"),
        bigquery.SchemaField("q3", "FLOAT"),
        bigquery.SchemaField("mode", "FLOAT"),
        bigquery.SchemaField("geometric_mean", "FLOAT"),
        bigquery.SchemaField("geometric_std", "FLOAT"),
        bigquery.SchemaField("geometric_conf_int_95_low", "FLOAT"),
        bigquery.SchemaField("geometric_conf_int_95_upp", "FLOAT"),
        bigquery.SchemaField("geometric_conf_int_75_low", "FLOAT"),
        bigquery.SchemaField("geometric_conf_int_75_upp", "FLOAT"),
        bigquery.SchemaField("geometric_conf_int_50_low", "FLOAT"),
        bigquery.SchemaField("geometric_conf_int_50_upp", "FLOAT"),
        bigquery.SchemaField("number_of_properties", "INTEGER"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    job.result()
    print(f"Table {table_id} updated successfully.")
                        
def main():
    with open('config/stats_config.json', 'r') as f:
        stats_config = json.load(f)
    with open('config/bigquery_config.json', 'r') as f:
        bigquery_config = json.load(f)
    
    percentile_bounds = [stats_config['outliers_percentile_lower'], stats_config['outliers_percentile_upper']]
    min_properties_to_compute_stats = stats_config['min_properties_to_compute_stats']
    
    df = fetch_data(bigquery_config)
    stadsdeel_subdivision_map = generate_stadsdeel_subdivision_map(df)
    processed_df = process_data(df)
    
    stats_df = run_stats_computation(processed_df, percentile_bounds, stadsdeel_subdivision_map, min_properties_to_compute_stats)
    
    upload_dataframe_to_bigquery(stats_df, bigquery_config)
    
if __name__ == "__main__":
    main()
    