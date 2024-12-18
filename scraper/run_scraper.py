from scraper import Scraper
import json
from google.cloud import bigquery
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

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
    tmp_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.tmp_property"
    property_table_id = f"{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.property"
    current_date = datetime.now(tz=ZoneInfo("Europe/Amsterdam")).strftime('%Y-%m-%d')

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("page_source", "STRING"),
            bigquery.SchemaField("scrape_date", "DATE"),
            bigquery.SchemaField("post_type", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("postcode", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("property_type", "STRING"),
            bigquery.SchemaField("price", "INTEGER"),
            bigquery.SchemaField("price_type", "STRING"),
            bigquery.SchemaField("surface", "INTEGER"),
            bigquery.SchemaField("surface_unit", "STRING"),
            bigquery.SchemaField("rooms", "INTEGER"),
            bigquery.SchemaField("furnished", "STRING"),
            bigquery.SchemaField("url", "STRING"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_dataframe(s.properties, tmp_table_id, job_config=job_config)
    load_job.result()

    # Query to merge tmp_property to property
    query = f"""
    MERGE `{property_table_id}` AS property
    USING `{tmp_table_id}` AS tmp
    ON property.url = tmp.url
    WHEN MATCHED THEN
    UPDATE SET 
        property.last_scrape_date = DATE("{current_date}"),
    WHEN NOT MATCHED THEN
    INSERT (
        page_source, post_type, city, location, postcode, title, property_type, price, price_type, 
        surface, surface_unit, rooms, furnished, url, first_scrape_date, last_scrape_date
    )
    VALUES (
        tmp.page_source, tmp.post_type, tmp.city, tmp.location, tmp.postcode, 
        tmp.title, tmp.property_type, tmp.price, tmp.price_type, tmp.surface, tmp.surface_unit, tmp.rooms, 
        tmp.furnished, tmp.url, DATE("{current_date}"), DATE("{current_date}")
    )
    """
    job = client.query(query)
    job.result()

    client.delete_table(tmp_table_id, not_found_ok=True)
        
except Exception as e:
    logging.exception(f"Scraper error: {e}")
