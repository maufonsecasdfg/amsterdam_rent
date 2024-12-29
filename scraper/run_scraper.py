from scraper import Scraper
import json
from google.cloud import bigquery
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('config/scraper_config.json', 'r') as f:
    scraper_config = json.load(f)
with open('config/bigquery_config.json', 'r') as f:
    bigquery_config = json.load(f)

logging.info("Starting scraper...")
try:
    s = Scraper()
    s.run(**scraper_config)

    client = bigquery.Client()
    property_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property"
    current_date = datetime.now(tz=ZoneInfo("Europe/Amsterdam")).strftime('%Y-%m-%d')
    
    update_query = f"""
        UPDATE `{property_table_id}`
        SET status = 'Unavailable'
        WHERE status = 'Available' AND last_scrape_date != DATE("{current_date}")
    """
    job = client.query(update_query)
    job.result()
        
except Exception as e:
    logging.exception(f"Scraper error: {e}")
    
# Deduplicate proerty table for entries found in different sources
query = f"""
WITH FilteredData AS (
    SELECT 
        a.url AS url_1,
        a.page_source AS page_source_1,
        a.postcode AS postcode_1,
        a.price AS price_1,
        a.rooms AS rooms_1,
        a.property_type AS property_type_1,
        a.title AS title_1,
        b.url AS url_2,
        b.page_source AS page_source_2,
        b.postcode AS postcode_2,
        b.price AS price_2,
        b.rooms AS rooms_2,
        b.property_type AS property_type_2,
        b.title AS title_2
    FROM `{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property` a
    JOIN `{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property` b
    ON a.postcode = b.postcode
    AND a.price = b.price
    AND a.rooms = b.rooms
    AND a.page_source = 'Pararius'
    WHERE a.page_source != b.page_source
)
SELECT * FROM FilteredData
"""

df = client.query(query).to_dataframe()

df['cleaned_title_1'] = df.apply(lambda x: x.title_1.replace(x.property_type_1, '').strip().replace('-',' ').lower(), axis=1)
df['cleaned_title_2'] = df.apply(lambda x: x.title_2.replace(x.property_type_2, '').strip().replace('-',' ').lower(), axis=1)

to_remove = list(df[(df['cleaned_title_1'] == df['cleaned_title_2'])]['url_1'].values)
df_to_remove = pd.DataFrame({'url': to_remove})

temp_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.temp_urls_to_remove"
job = client.load_table_from_dataframe(df_to_remove, temp_table_id)
job.result()

print("Temporary table with URLs to remove created.")

delete_query = f"""
DELETE FROM `{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property`
WHERE page_source = 'Pararius'
AND url IN (SELECT url FROM `{temp_table_id}`)
"""

job = client.query(delete_query)
job.result()

print("Duplicate Pararius entries removed successfully.")

drop_query = f"DROP TABLE `{temp_table_id}`"
client.query(drop_query).result()

print("Temporary table dropped.")

