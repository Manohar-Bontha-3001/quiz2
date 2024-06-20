import os
from flask import Flask, request, render_template
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, text
from geopy.distance import geodesic
from datetime import datetime, timedelta
from statistics import mean
# from pymemcache.client.base import Client
import pyodbc
import random
import string

# Set up Matplotlib
import matplotlib
matplotlib.use('Agg')

# Define the Flask app
app = Flask(__name__)
app.secret_key = os.urandom(24)

# SQLAlchemy connection string
connection_string = (
    "mssql+pyodbc://manoharb:Arjunsuha1*@manoharb.database.windows.net:1433/manoharb?driver=ODBC+Driver+17+for+SQL+Server"
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
          AND geography::Point(latitude, longitude, 4326).STDistance(geography::Point(?, ?, 4326)) <= ?
          AND place_name LIKE ?
          AND (DATEPART(HOUR, [time]) >= 18 OR DATEPART(HOUR, [time]) <= 6)
    """
    params = (min_mag, max_mag, start_date, end_date, latitude, longitude, distance, f"%{place}%")

    # Execute the query using the SQLAlchemy engine
    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        results = result.fetchall()
    
    return results


def measure_query_time(query_function, *args, **kwargs):
    start_time = datetime.now()
    results = query_function(*args, **kwargs)
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    return results, elapsed_time.total_seconds()

# def query_with_caching(query_key, query_function, *args, **kwargs):
#     cached_results = mc.get(query_key)
#     if cached_results is None:
#         results = query_function(*args, **kwargs)
#         mc.set(query_key, results, time=3600)  # Cache for 1 hour
#         return results, False  # False indicates the results were not cached
#     else:
#         return cached_results, True  # True indicates the results were retrieved from cache
from statistics import mean

@app.route('/random_queries', methods=['GET'])
def random_queries():
    try:
        # Define the maximum number of random queries
        max_queries = 1000

        # Initialize an empty list to store query results
        query_results = []

        # Generate and execute random queries
        for _ in range(max_queries):
            # Generate random query parameters
            min_mag = random.uniform(0, 10)
            max_mag = random.uniform(min_mag, 10)
            start_date = datetime.now() - timedelta(days=random.randint(1, 365))
            end_date = start_date + timedelta(days=random.randint(1, 365))
            latitude = random.uniform(-90, 90)
            longitude = random.uniform(-180, 180)
            place = ''.join(random.choices(string.ascii_letters, k=random.randint(5, 15)))
            distance = random.uniform(0, 1000)
            night_time = random.choice([True, False])

            # Execute the query and measure query time
            results, elapsed_time = measure_query_time(perform_query, {
                'min_mag': min_mag,
                'max_mag': max_mag,
                'start_date': start_date,
                'end_date': end_date,
                'latitude': latitude,
                'longitude': longitude,
                'place': place,
                'distance': distance,
                'night_time': night_time
            })

            # Append query results to the list
            query_results.append({'query_parameters': {
                'min_mag': min_mag,
                'max_mag': max_mag,
                'start_date': start_date,
                'end_date': end_date,
                'latitude': latitude,
                'longitude': longitude,
                'place': place,
                'distance': distance,
                'night_time': night_time
            }, 'results': results, 'query_time': elapsed_time})

        # Render the query_results.html template with the query results data
        return render_template('query_results.html', query_results=query_results)
    except Exception as e:
        return str(e), 400


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/query', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        try:
            start_time = datetime.now()  # Start measuring time
            
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
                end_time = datetime.now()  # End measuring time
                elapsed_time = (end_time - start_time).total_seconds()  # Calculate elapsed time
                return render_template('results.html', earthquakes=nearby_earthquakes, map_path=None, query_time=elapsed_time)

            if place:
                query += ' AND [place_name] LIKE ?'
                params.append(f'%{place}%')

            if distance:
                query += ' AND [distance] <= ?'
                params.append(float(distance))

            if night_time:
                query += " AND [mag] > 4.0 AND (DATEPART(HOUR, [time]) >= 18 OR DATEPART(HOUR, [time]) <= 6)"

            query_key = f"{min_mag}_{max_mag}_{start_date}_{end_date}_{lat}_{lon}_{place}_{distance}_{night_time}"
            try:
                earthquakes = execute_query(query, tuple(params))
                end_time = datetime.now()  # End measuring time
                elapsed_time = (end_time - start_time).total_seconds()  # Calculate elapsed time
                return render_template('results.html', earthquakes=earthquakes, map_path=None, cached=False, query_time=elapsed_time)
            except Exception as e:
                return str(e), 400

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

@app.route('/create_table')
def create_table():
    try:
        # Generate a random table name
        table_name = 'earthquake_' + ''.join(random.choices(string.ascii_letters + string.digits, k=2))

        # Define your table schema
        metadata = MetaData()
        earthquakes = Table(
            table_name, metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('time', DateTime),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('mag', Float),
            Column('place', String),
            Column('distance', Float),
            Column('place_name', String),
        )

        # Measure time to create the table
        start_time = datetime.now()
        metadata.create_all(engine)
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        return f"Table '{table_name}' created successfully in {elapsed_time.total_seconds()} seconds."
    except Exception as e:
        return str(e), 400

if __name__ == '__main__':
    app.run(debug=True)
