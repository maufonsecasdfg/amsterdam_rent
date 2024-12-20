from flask import Flask, request, render_template_string
from google.cloud import bigquery
import folium
import pandas as pd
import geopandas as gpd
from shapely import wkt
import json
import branca.colormap as cm

app = Flask(__name__)

with open('config/bigquery_config.json', 'r') as f:
    bigquery_config = json.load(f)
    
with open('config/folium_config.json', 'r') as f:
    folium_config = json.load(f)

@app.route("/")
def index():
    # Extract filter parameters
    region_resolution = request.args.get('region_resolution', 'stadsdeel')
    post_type = request.args.get('post_type', 'Buy')
    property_type = request.args.get('property_type', 'All')
    property_type_selections = request.args.getlist("property_type")
    if set(property_type_selections) == {"Apartment", "House"}:
        property_type = "All"
    elif len(property_type_selections) == 1:
        # If only one is selected, use that one
        property_type = property_type_selections[0]
    else:
        # If none selected or any unexpected scenario, default to "All"
        property_type = "All"
    furnished = request.args.get('furnished', '')
    value = request.args.get('value', 'price')
    metric = request.args.get('metric', 'geometric_mean')
    confidence_interval = request.args.get('confidence_interval', 75)
    
    region_resolution_column = 'stadsdeel' if region_resolution == 'stadsdeel' else 'stadsdeel_onderverdeling'
    
    if not region_resolution:
        region_resolution = 'stadsdeel'
    if not post_type:
        post_type = 'Buy'
    if not property_type:
        post_type = 'All'
    if post_type == 'Buy':
        furnished = None
    if post_type == 'Rent' and not furnished:
        furnished = 'All'
    if not value:
        value = 'price'
    if not metric:
        metric = 'geometric_mean'
    
    if metric == 'geometric_mean':
        select = f"""
        SELECT 
        s.region_resolution as region_resolution, 
        s.stadsdeel as stadsdeel, 
        s.subdivision as subdivision,
        s.geometric_mean as geometric_mean, 
        s.geometric_std as geometric_std, 
        s.geometric_conf_int_{confidence_interval}_low as geometric_conf_int_{confidence_interval}_low,  
        s.geometric_conf_int_{confidence_interval}_upp as geometric_conf_int_{confidence_interval}_upp, 
        s.number_of_properties as number_of_properties,
        g.geometry as geometry
        """
    elif metric == 'median':
        select = """
        SELECT 
        s.region_resolution as region_resolution, 
        s.stadsdeel as stadsdeel, 
        s.subdivision as subdivision,
        s.median as median, 
        s.q1 as q1, 
        s.q3 as q3, 
        s.number_of_properties as number_of_properties,
        g.geometry as geometry
        """
    
    elif metric == 'mode':
        select = """
        SELECT 
        s.region_resolution as region_resolution, 
        s.stadsdeel as stadsdeel, 
        s.subdivision as subdivision,
        s.mode as mode, 
        s.number_of_properties as number_of_properties,
        g.geometry as geometry
        """

    # Build a BigQuery query with these parameters
    # Adjust this query as per your actual schema and filters
    client = bigquery.Client()
    query = f"""
    {select}
    FROM `{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.stats` s
    LEFT JOIN `{bigquery_config['project_id']}.{bigquery_config['dataset_id']}.geodata_{region_resolution_column}` g
    ON s.{region_resolution} = g.{region_resolution_column}
    WHERE 1=1
    """

    # Add conditions based on filters
    query += f" AND region_resolution = '{region_resolution_column}'"
    query += f" AND post_type = '{post_type}'"
    query += f" AND property_type = '{property_type}'"
    if furnished:
        query += f" AND furnished = '{furnished}'"
    query += f" AND value = '{value}'"
    
    print(query)
    
    df = client.query(query).to_dataframe()
    df["geometry"] = df["geometry"].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:28992")
    gdf['stadsdeel'] = gdf['stadsdeel'].str.replace('Stadsdeel ', '',regex=False)


    # Initialize map
    m = folium.Map(location=folium_config['location'], zoom_start=folium_config['zoom_start'], tiles=folium_config['tiles'])
    
    colormap = cm.LinearColormap(["yellow", "orange", "red"], vmin=gdf[metric].min(), vmax=gdf[metric].max())
    metirc_dict = gdf.set_index(region_resolution)[metric].fillna(0)
    
    def style_function(feature):
        value = metirc_dict[feature['properties'][region_resolution]]
        if value == 0:
            fill_color = 'grey'  # Color for regions with no data
        else:
            fill_color = colormap(value)
        return {
            'fillColor': fill_color,
            'color': 'black',
            'weight': 0.7,
            'fillOpacity': 0.3
        }
    
    fields = ['stadsdeel']
    aliases = ['Stadsdeel']
    if region_resolution == 'subdivision':
        fields += ['subdivision']
        aliases += ['Subdivision:']
    fields += [metric]
    if metric == 'geometric_mean':
        aliases += ['Mean:']
    if metric == 'median':
        aliases += ['Median:']
    if metric == 'mode':
        aliases += ['Mode:']
    fields += ['number_of_properties']
    aliases += ['Number of properties:']
    folium.GeoJson(
        gdf,
        name="Regions",
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(
            fields=fields,  
            aliases=aliases,   
            localize=True,
        )
    ).add_to(m)
    colormap.add_to(m)
    
    folium.LayerControl().add_to(m)
    
    map_html = m._repr_html_()


    html_template = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <title>Amsterdam Real Estate Visualizer</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <!-- Bootstrap CSS (CDN) -->
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        <style>
            body {{
                margin: 20px;
            }}
            .filter-form {{
                margin-bottom: 20px;
            }}
            #map-container {{
                height: 600px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="my-4">Map Visualization</h1>
            <form method="get" action="/" class="filter-form">
                <div class="row">
                    <!-- Region Resolution (Radio) -->
                    <div class="form-group col-md-3">
                        <label>Region Resolution</label><br>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="region_resolution" id="stadsdeel" value="stadsdeel" {"checked" if region_resolution=="stadsdeel" else ""}>
                            <label class="form-check-label" for="stadsdeel">Stadsdeel</label>
                        </div>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="region_resolution" id="subdivision" value="subdivision" {"checked" if region_resolution=="subdivision" else ""}>
                            <label class="form-check-label" for="subdivision">Stadsdeel Subdivision</label>
                        </div>
                    </div>
                    
                    <!-- Post Type (Radio) -->
                    <div class="form-group col-md-3">
                        <label>Post Type</label><br>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="post_type" id="buy" value="Buy" {"checked" if post_type=="Buy" else ""} onchange="toggleFurnishedOptions()">
                            <label class="form-check-label" for="buy">Buy</label>
                        </div>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="post_type" id="rent" value="Rent" {"checked" if post_type=="Rent" else ""} onchange="toggleFurnishedOptions()">
                            <label class="form-check-label" for="rent">Rent</label>
                        </div>
                    </div>
                    
                    <!-- Property Type (Checkboxes) -->
                    <div class="form-group col-md-3">
                        <label>Property Type</label><br>
                        <!-- Determine check states -->
                        <!-- If property_type == 'All' both checked -->
                        <!-- If property_type == 'Apartment' only that checked -->
                        <!-- If property_type == 'House' only that checked -->
                        <div class="form-check">
                            <input class="form-check-input" type="checkbox" name="property_type" id="apartment" value="Apartment"
                            {"checked" if property_type in ["All", "Apartment"] else ""}>
                            <label class="form-check-label" for="apartment">Apartment</label>
                        </div>
                        <div class="form-check">
                            <input class="form-check-input" type="checkbox" name="property_type" id="house" value="House"
                            {"checked" if property_type in ["All", "House"] else ""}>
                            <label class="form-check-label" for="house">House</label>
                        </div>
                    </div>
                    
                    <!-- Furnished (Radio) - Only show if post_type == "Rent" -->
                    <div class="form-group col-md-3" id="furnished-options" style="display: none;">
                        <label>Furnished</label><br>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="furnished" id="furnished_all" value="All" {"checked" if furnished=="All" else ""}>
                            <label class="form-check-label" for="furnished_all">All</label>
                        </div>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="furnished" id="furnished_furnished" value="Furnished" {"checked" if furnished=="Furnished" else ""}>
                            <label class="form-check-label" for="furnished_furnished">Furnished</label>
                        </div>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="furnished" id="furnished_upholstered" value="Upholstered" {"checked" if furnished=="Upholstered" else ""}>
                            <label class="form-check-label" for="furnished_upholstered">Upholstered</label>
                        </div>
                        <div class="form-check">
                            <input class="form-check-input" type="radio" name="furnished" id="furnished_shell" value="Shell" {"checked" if furnished=="Shell" else ""}>
                            <label class="form-check-label" for="furnished_shell">Shell</label>
                        </div>
                    </div>
                </div>
                
                <div class="row">
                    <!-- Value (Dropdown) -->
                    <div class="form-group col-md-3">
                        <label for="value">Value</label>
                        <select class="form-control" name="value" id="value">
                            <option value="price" {"selected" if value=="price" else ""}>price</option>
                            <option value="price_per_m2" {"selected" if value=="price_per_m2" else ""}>price_per_m2</option>
                            <option value="price_per_room" {"selected" if value=="price_per_room" else ""}>price_per_room</option>
                            <option value="surface" {"selected" if value=="surface" else ""}>surface</option>
                        </select>
                    </div>
                    
                    <!-- Metric (Dropdown) -->
                    <div class="form-group col-md-3">
                        <label for="metric">Metric</label>
                        <select class="form-control" name="metric" id="metric">
                            <option value="geometric_mean" {"selected" if metric=="geometric_mean" else ""}>geometric_mean</option>
                            <option value="median" {"selected" if metric=="median" else ""}>median</option>
                            <option value="mode" {"selected" if metric=="mode" else ""}>mode</option>
                        </select>
                    </div>
                </div>
                
                <button type="submit" class="btn btn-primary">Apply Filters</button>
            </form>

            <div id="map-container">
                {map_html}
            </div>
        </div>

        <!-- Bootstrap JS (CDN), Popper.js, and jQuery -->
        <script src="https://code.jquery.com/jquery-3.5.1.slim.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.1/dist/umd/popper.min.js"></script>
        <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js"></script>
        <script>
            function toggleFurnishedOptions() {{
                const postType = document.querySelector('input[name="post_type"]:checked').value;
                const furnishedDiv = document.getElementById('furnished-options');
                if (postType === 'Rent') {{
                    furnishedDiv.style.display = 'block';
                }} else {{
                    furnishedDiv.style.display = 'none';
                }}
            }}
            // Initialize on page load
            toggleFurnishedOptions();
        </script>
    </body>
    </html>
    """


    return render_template_string(html_template)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
