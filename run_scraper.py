from scraper import Scraper
import json
from google.cloud import bigquery
from datetime import datetime
from zoneinfo import ZoneInfo

with open('config/scraper_config.json', 'r') as f:
    scraper_config = json.load(f)
with open('config/bigquery_config.json', 'r') as f:
    bigquery_config = json.load(f)

s = Scraper()
s.run(**scraper_config)

client = bigquery.Client()
tmp_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.tmp_property"
property_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property"
postcode_coordinates_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.postcode_coordinates"
current_date = datetime.now(tz=ZoneInfo("Europe/Amsterdam")).strftime('%Y-%m-%d')

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("scrape_date", "DATE"),
        bigquery.SchemaField("post_type", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("postcode", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("price", "INT"),
        bigquery.SchemaField("price_type", "STRING"),
        bigquery.SchemaField("surface", "INT"),
        bigquery.SchemaField("surface_unit", "STRING"),
        bigquery.SchemaField("rooms", "INT"),
        bigquery.SchemaField("furnished", "STRING"),
        bigquery.SchemaField("url", "STRING"),
    ],
    write_disposition="WRITE_TRUNCATE",
)

load_job = client.load_table_from_dataframe(s.property, tmp_table_id, job_config=job_config)
load_job.result()

# Query to merge tmp_property to property
query = f"""
MERGE `{property_table_id}` AS property
USING `{tmp_table_id}` AS tmp
ON property.url = tmp.url
WHEN MATCHED THEN
  UPDATE SET 
    property.last_scrape_date = DATE("{current_date}")
WHEN NOT MATCHED THEN
  INSERT (
    source, scrape_date, post_type, city, location, postcode, title, price, price_type, 
    surface, surface_unit, rooms, furnished, url, first_scrape_date, last_scrape_date, 
    longitude, latitude
  )
  VALUES (
    tmp.source, tmp.scrape_date, tmp.post_type, tmp.city, tmp.location, tmp.postcode, 
    tmp.title, tmp.price, tmp.price_type, tmp.surface, tmp.surface_unit, tmp.rooms, 
    tmp.furnished, tmp.url, DATE("{current_date}"), DATE("{current_date}"), 
    NULL, NULL
  )
"""
job = client.query(query)
job.result()

client.delete_table(tmp_table_id, not_found_ok=True)

query = f"""
MERGE `{property_table_id}` AS property
USING (
  SELECT * 
  FROM `{postcode_coordinates_table_id}` AS coords
  WHERE coords.postcode IN (
    SELECT postcode 
    FROM `{property_table_id}` 
    WHERE latitude IS NULL
  )
) AS coords
ON property.postcode = coords.postcode
WHEN MATCHED THEN
  UPDATE SET 
    property.latitude = coords.latitude,
    property.longitude = coords.longitude
"""

job = client.query(query)
job.result()












