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
