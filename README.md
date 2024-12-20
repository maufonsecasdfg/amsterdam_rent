# Amsterdam Rent Visualizer

## Overview
The Amsterdam Rent Visualizer is a project designed to gather real estate listing data (rental and for-sale properties) in Amsterdam and present it in a statistical dashboard. Users can explore metrics such as average prices, price per square meter, and more, filtered by various factors like location (stadsdeel, subdivisions, and, in the future, wijken and buurten), property size, number of rooms, and furnishing status.

Currently, due to limited data, only stadsdelen and subdivisions are available in the dashboard. More granular subdivisions (e.g., wijken and buurten) are under development as data collection continues.

## Repository Structure
- **scraper/**  
  Contains code to scrape data from real estate platforms (currently Pararius; Funda planned). Integrates with Google Cloud and BigQuery for data storage.

- **statistics/**  
  Code to pull raw data from BigQuery, compute aggregated metrics (e.g., average rent, average selling price), and push these statistics back into BigQuery for the app to consume.

- **geometry/**  
  Tools to map property listings (often identified by postal code) to spatial divisions like stadsdelen and smaller units, and generate geometry data for visualization. Uses official data sources from [CBS](https://www.cbs.nl).

- **app/**  
  Source code for the interactive dashboard that displays Amsterdam’s map with overlaid metrics. Users can filter by location and property attributes. Currently deployed at:
  [https://amsterdam-app-397124807552.europe-west4.run.app](https://amsterdam-app-397124807552.europe-west4.run.app).

**Note:** The `scraper`, `statistics`, and `app` directories each contain a `Dockerfile` to facilitate deployment on Google Cloud services.

## Future Improvements
- Integrate additional data sources (e.g., Funda).
- Increase spatial granularity as more data becomes available.
- Add richer data exploration features in the app.
- Implement performance improvements for large datasets.