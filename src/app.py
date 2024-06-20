import os
import pandas as pd
from flask import Flask, request, render_src, redirect, url_for
from sqlalchemy import create_engine, text
from geopy.distance import geodesic
import matplotlib
matplotlib.use('Agg')  # Force Agg backend
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import pyodbc 
from selenium import webdriver
from jinja2 import Environment, FileSystemLoader

# Get the absolute path to the src directory
base_dir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Generates a random secret key

# Replace {your_password_here} with your actual password
connection_string = (
    "mssql+pyodbc://manoharb:Arjunsuha1*@manoharb.database.windows.net:1433/manoharb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create SQLAlchemy engine
engine = create_engine(connection_string)

def setup_matplotlib():
    import matplotlib.pyplot as plt
    plt.switch_backend('Agg')  # Ensure Agg backend
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

setup_matplotlib()

# Function to execute SQL queries
def execute_query(query, params=None):
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=manoharb.database.windows.net;'
        'DATABASE=manoharb;'
        'UID=manoharb;'
        'PWD=Arjunsuha1*'
    )
    cursor = connection.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows

def generate_map(earthquakes, map_path):
    # Generate a map visualization for the provided earthquakes
    plt.figure(figsize=(12, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.set_global()
    for quake in earthquakes:
        plt.plot(quake['Longitude'], quake['Latitude'], 'ro', markersize=5, transform=ccrs.PlateCarree())
    plt.title('Earthquake Locations')
    plt.savefig(map_path)
    plt.close()

@app.route('/')
def index():
    template_dir = '/Users/bhavya/Downloads/qz/src'
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('index.html')
    return template.render()

@app.route('/query', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        try:
            min_mag = request.form.get('min_mag')
            max_mag = request.form.get('max_mag')
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
            lat = request.form.get('latitude')
            lon = request.form.get('longitude')
            place = request.form.get('place')
            distance = request.form.get('distance')
            night_time = request.form.get('night_time')

            query = '''
                SELECT [time], [latitude], [longitude], [mag], [place], [distance], [place_name]
                FROM [dbo].[earthquakes]
                WHERE 1=1
            '''
            params = []

            if min_mag and max_mag:
                query += ' AND [mag] BETWEEN ? AND ?'
                params.extend([float(min_mag), float(max_mag)])

            if start_date and end_date:
                if start_date <= end_date:
                    query += ' AND [time] BETWEEN ? AND ?'
                    params.extend([start_date, end_date])
                else:
                    return 'Error: Start date must be before end date.', 400

            if lat and lon and distance:
                try:
                    distance = float(distance)
                except ValueError:
                    return 'Error: Distance must be a number.', 400

                earthquakes = execute_query(query, tuple(params))
                nearby_earthquakes = [
                    {
                        'Datetime': quake[0],
                        'Latitude': float(quake[1]),
                        'Longitude': float(quake[2]),
                        'Magnitude': float(quake[3]),
                        'Place': quake[4],
                        'Distance': float(quake[5]),
                        'Place_Name': quake[6]
                    }
                    for quake in earthquakes
                    if geodesic((float(lat), float(lon)), (float(quake[1]), float(quake[2]))).km <= distance
                ]
                map_path = os.path.join(base_dir, 'map.png')
                generate_map(nearby_earthquakes, map_path)
                return render_src('results.html', earthquakes=nearby_earthquakes, map_path='map.png')

            if place:
                query += ' AND [place_name] LIKE ?'
                params.append(f'%{place}%')

            if distance:
                query += ' AND [distance] <= ?'
                params.append(float(distance))

            if night_time:
                query += " AND [mag] > 4.0 AND (DATEPART(HOUR, [time]) >= 18 OR DATEPART(HOUR, [time]) <= 6)"

            earthquakes = execute_query(query, tuple(params))
            map_path = os.path.join(base_dir, 'map.png')
            generate_map(earthquakes, map_path)
            return render_src('results.html', earthquakes=earthquakes, map_path='map.png')

        except Exception as e:
            return str(e), 400

    return render_src('query.html')

@app.route('/count_large_earthquakes', methods=['GET'])
def count_large_earthquakes():
    try:
        result = execute_query('SELECT COUNT(*) AS count FROM earthquakes WHERE Mag > 5.0')
        count = result[0][0]  # Accessing the first column directly
        return f'Total earthquakes with magnitude greater than 5.0: {count}'
    except Exception as e:
        return str(e), 400

@app.route('/large_earthquakes_night', methods=['GET'])
def large_earthquakes_night():
    try:
        result = execute_query('''
            SELECT COUNT(*) AS count 
            FROM earthquakes 
            WHERE Mag > 4.0 
            AND (DATEPART(HOUR, datetime) >= 18 OR DATEPART(HOUR, datetime) <= 6)
        ''')
        count = result[0][0]  # Accessing the first column directly
        return f'Total large earthquakes (>4.0 mag) at night: {count}'
    except Exception as e:
        return str(e), 400

@app.route('/find_clusters', methods=['GET'])
def find_clusters():
    try:
        query = '''
            SELECT datetime AS Datetime, latitude AS Latitude, longitude AS Longitude, Mag AS Magnitude, place AS Place, distance AS Distance, place_name AS Place_Name
            FROM earthquakes
        '''
        earthquakes = execute_query(query)
        clusters = []
        for quake in earthquakes:
            cluster = [q for q in earthquakes if geodesic((quake['Latitude'], quake['Longitude']), (q['Latitude'], q['Longitude'])).km <= 50]
            if len(cluster) > 1:
                clusters.append(cluster)
        map_path = os.path.join(base_dir, 'map.png')
        generate_map([item for sublist in clusters for item in sublist], map_path)
        return render_src('clusters.html', clusters=clusters, map_path='map.png')
    except Exception as e:
        return str(e), 400

if __name__ == '__main__':
    app.run(debug=True)
