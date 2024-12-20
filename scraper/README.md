# Scraper

## Overview
The `scraper` directory contains scripts and tools for extracting property listings from real estate websites (currently Pararius). It gathers data such as price, dates of offering, surface area, number of rooms, furnishing status, and location. The collected data is stored in BigQuery.

## Key Components
- **Scraper Scripts**: Python code that fetches and parses listings data from APIs or HTML pages.
- **Data Validation**: Steps to ensure accuracy and consistency before loading into BigQuery.
- **BigQuery Integration**: Queries and scripts for table creation, data insertion, and schema management.

## Deployment
- A `Dockerfile` is provided for containerization.
- The scraper can be scheduled to run periodically using Google Cloud Scheduler, keeping the dataset up-to-date.
- Deployed project scrapes for Amsterdam only, but the code allows to select scraping areas on the gemeente (municipality) level. One can set up the scraping of one or more municipalities.

## Future Plans
- Add a Funda scraper to diversify data sources.
- Track historical changes in property listings over time.