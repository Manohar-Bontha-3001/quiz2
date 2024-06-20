import os
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for
from sqlalchemy import create_engine, text
from geopy.distance import geodesic
import matplotlib
matplotlib.use('Agg')  # Force Agg backend
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import pyodbc

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
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    return rows

def generate_map(earthquakes):
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()})
    ax.stock_img()
    ax.coastlines()
    
    for quake in earthquakes:
        ax.plot(quake['Longitude'], quake['Latitude'], marker='o', color='red', markersize=5, transform=ccrs.Geodetic())

    map_path = 'static/map.png'
    plt.savefig(map_path)
    
    if not os.path.exists(map_path):
        print("Map file was not created")
    
    return map_path

def perform_query(form_data):
    min_mag = form_data.get('min_mag', 0)
    max_mag = form_data.get('max_mag', 10)
    start_date = form_data.get('start_date', '1970-01-01')
    end_date = form_data.get('end_date', '2100-01-01')
    latitude = form_data.get('latitude')
    longitude = form_data.get('longitude')
    place = form_data.get('place')
    distance = form_data.get('distance')
    night_time = form_data.get('night_time', False)
    
    query = """
        SELECT *
        FROM earthquakes
        WHERE mag BETWEEN ? AND ?
          AND [time] BETWEEN ? AND ?
    """
    params = [min_mag, max_mag, start_date, end_date]
    
    if latitude and longitude and distance:
        query += " AND ST_Distance_Sphere(point(longitude, latitude), point(?, ?)) <= ?"
        params.extend([longitude, latitude, distance])
    
    if place:
        query += " AND place_name LIKE ?"
        params.append(f"%{place}%")
    
    if night_time:
        query += " AND (DATEPART(HOUR, [time]) >= 18 OR DATEPART(HOUR, [time]) <= 6)"
    
    cursor = engine.execute(query, params)
    results = cursor.fetchall()
    return results

@app.route('/')
def index():
    return render_template('index.html')

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
                SELECT [time] AS Datetime, [latitude] AS Latitude, [longitude] AS Longitude, [mag] AS Magnitude, [place] AS Place, [distance] AS Distance, [place_name] AS Place_Name
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

                earthquakes = perform_query(request.form)
                nearby_earthquakes = [
                    {
                        'Datetime': quake['Datetime'],
                        'Latitude': float(quake['Latitude']),
                        'Longitude': float(quake['Longitude']),
                        'Magnitude': float(quake['Magnitude']),
                        'Place': quake['Place'],
                        'Distance': float(quake['Distance']),
                        'Place_Name': quake['Place_Name']
                    }
                    for quake in earthquakes
                    if geodesic((float(lat), float(lon)), (float(quake['Latitude']), float(quake['Longitude']))).km <= distance
                ]
                map_path = generate_map(nearby_earthquakes)
                return render_template('results.html', earthquakes=nearby_earthquakes, map_path=map_path)

            if place:
                query += ' AND [place_name] LIKE ?'
                params.append(f'%{place}%')

            if distance:
                query += ' AND [distance] <= ?'
                params.append(float(distance))

            if night_time:
                query += " AND [mag] > 4.0 AND (DATEPART(HOUR, [time]) >= 18 OR DATEPART(HOUR, [time]) <= 6)"

            earthquakes = execute_query(query, tuple(params))
            map_path = generate_map(earthquakes)
            return render_template('results.html', earthquakes=earthquakes, map_path=map_path)

        except Exception as e:
            return str(e), 400

    return render_template('query.html')

@app.route('/count_large_earthquakes', methods=['GET'])
def count_large_earthquakes():
    try:
        result = execute_query('SELECT COUNT(*) AS count FROM earthquakes WHERE mag > 5.0')
        count = result[0]['count']  # Accessing the first column directly
        return f'Total earthquakes with magnitude greater than 5.0: {count}'
    except Exception as e:
        return str(e), 400

@app.route('/large_earthquakes_night', methods=['GET'])
def large_earthquakes_night():
    try:
        result = execute_query('''
            SELECT COUNT(*) AS count 
            FROM earthquakes 
            WHERE mag > 4.0 
            AND (DATEPART(HOUR, time) >= 18 OR DATEPART(HOUR, time) <= 6)
        ''')
        count = result[0]['count']  # Accessing the first column directly
        return f'Total large earthquakes (>4.0 mag) at night: {count}'
    except Exception as e:
        return str(e), 400

@app.route('/find_clusters', methods=['GET'])
def find_clusters():
    try:
        query = '''
            SELECT [time] AS Datetime, [latitude] AS Latitude, [longitude] AS Longitude, [mag] AS Magnitude, [place] AS Place
            FROM [dbo].[earthquakes]
        '''
        earthquakes = execute_query(query)
        formatted_earthquakes = [
            {
                'Datetime': quake['Datetime'],
                'Latitude': float(quake['Latitude']),
                'Longitude': float(quake['Longitude']),
                'Magnitude': float(quake['Magnitude']),
                'Place': quake['Place']
            }
            for quake in earthquakes
        ]

        clusters = []
        for quake in formatted_earthquakes:
            cluster = [q for q in formatted_earthquakes if geodesic((quake['Latitude'], quake['Longitude']), (q['Latitude'], q['Longitude'])).km <= 50]
            if len(cluster) > 1:
                clusters.append(cluster)

        # Remove duplicate clusters
        unique_clusters = []
        for cluster in clusters:
            if cluster not in unique_clusters:
                unique_clusters.append(cluster)

        return render_template('clusters.html', clusters=unique_clusters)
    except Exception as e:
        return str(e), 400

if __name__ == '__main__':
    app.run(debug=True)