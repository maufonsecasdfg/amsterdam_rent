# App

## Overview
Contains the source code for the interactive Amsterdam Real Estate Visualizer dashboard. It displays a map of Amsterdam with overlaid metrics, allowing users to filter by post type (rent vs buy), property type (apartment vs house) and furnishings. The app fetches data from BigQuery, where statistics have been precomputed by the `statistics` module, and uses the geometry data to render spatial boundaries.

## Key Components
- **Front-End Components**: Map visualization and interactive filters.
- **Back-End Integration**: Queries BigQuery for the latest aggregated metrics.
- **Filtering & Parameters**: Users can refine views based on various attributes, ensuring a tailored exploration experience.

## Deployment
- A `Dockerfile` is included for containerization.
- The app is currently deployed at:  
  [https://amsterdam-app-397124807552.europe-west4.run.app](https://amsterdam-app-397124807552.europe-west4.run.app) [UNDER DEVELOPMENT]

## Future Plans
- Add more advanced filtering options and UI enhancements.
- Include histograms for the data in each region.
- Add more visualizations for statistical exploration (bar charts along with the heat-map).
- Integrate time-series visualizations to track trends over time.
