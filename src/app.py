import os
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for
from sqlalchemy import create_engine, text
import json
from redis import Redis

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Generates a random secret key

# Connection string
connection_string = (
    "mssql+pyodbc://manoharb:Arjunsuha1*@manoharb.database.windows.net:1433/manoharb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create SQLAlchemy engine
engine = create_engine(connection_string)
app.config['REDIS_URL'] = "redis://localhost:6379/0"
redis = Redis.from_url(app.config['REDIS_URL'])


def execute_query(query, params=None):
    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        return result.fetchall()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file:
        try:
            data = pd.read_csv(file)
            with engine.connect() as connection:
                for index, row in data.iterrows():
                    connection.execute(text('''
                        INSERT INTO earthquakes (
                            datetime, latitude, longitude, Magnitude, magType, nst, gap, dmin, rms, net, id_earthquake, updated, place, type, local_time
                        ) VALUES (:time, :latitude, :longitude, :Magnitude, :magType, :nst, :gap, :dmin, :rms, :net, :id_earthquake, :updated, :place, :type, :local_time)
                    '''), {
                        'time': row['time'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'Magnitude': row['mag'],
                        'magType': row['magType'],
                        'nst': row['nst'],
                        'gap': row['gap'],
                        'dmin': row['dmin'],
                        'rms': row['rms'],
                        'net': row['net'],
                        'id_earthquake': row['id'],
                        'updated': row['updated'],
                        'place': row['place'],
                        'type': row['type'],
                        'local_time': row['local_time']
                    })
            return redirect(url_for('index'))
        except Exception as e:
            return str(e), 400
    return 'No file uploaded', 400


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

            cache_key = f"{min_mag}_{max_mag}_{start_date}_{end_date}_{lat}_{lon}_{place}_{distance}_{night_time}"
            cached_results = redis.get(cache_key)

            if cached_results:
                earthquakes = json.loads(cached_results)
            else:
                query = '''
                    SELECT datetime AS Datetime, latitude AS Latitude, longitude AS Longitude, Magnitude AS Magnitude, place AS Place
                    FROM earthquakes
                    WHERE 1=1
                '''
                params = {}

                if min_mag and max_mag:
                    query += ' AND Magnitude BETWEEN :min_mag AND :max_mag'
                    params['min_mag'] = min_mag
                    params['max_mag'] = max_mag

                if start_date and end_date:
                    if start_date <= end_date:
                        query += ' AND datetime BETWEEN :start_date AND :end_date'
                        params['start_date'] = start_date
                        params['end_date'] = end_date
                    else:
                        return 'Error: Start date must be before end date.', 400

                if lat and lon:
                    query += ' AND latitude = :latitude AND longitude = :longitude'
                    params['latitude'] = lat
                    params['longitude'] = lon

                if place:
                    query += ' AND place LIKE :place'
                    params['place'] = f'%{place}%'

                if distance:
                    query += '''
                        AND TRY_CAST(LEFT(place, CHARINDEX(' km', place) - 1) AS INT) = :distance
                    '''
                    params['distance'] = distance

                if night_time:
                    query += " AND Magnitude > 4.0 AND (DATEPART(HOUR, datetime) >= 18 OR DATEPART(HOUR, datetime) <= 6)"

                earthquakes = execute_query(query, params)
                redis.set(cache_key, json.dumps([dict(row) for row in earthquakes]), ex=60*5)  # Cache for 5 minutes

            return render_template('results.html', earthquakes=earthquakes)

        except Exception as e:
            return str(e), 400

    return render_template('query.html')


@app.route('/count', methods=['GET'])
def count_large_earthquakes():
    try:
        result = execute_query('SELECT COUNT(*) AS count FROM earthquakes WHERE Magnitude > 5.0')
        count = result[0][0]  # Accessing the first column directly
        return f'Total earthquakes with magnitude greater than 5.0: {count}'
    except Exception as e:
        return str(e), 400


@app.route('/night', methods=['GET'])
def large_earthquakes_night():
    try:
        result = execute_query('''
            SELECT COUNT(*) AS count 
            FROM earthquakes 
            WHERE Magnitude > 4.0 
            AND (DATEPART(HOUR, datetime) >= 18 OR DATEPART(HOUR, datetime) <= 6)
        ''')
        count = result[0][0]  # Accessing the first column directly
        return f'Total large earthquakes (>4.0 mag) at night: {count}'
    except Exception as e:
        return str(e), 400


if __name__ == '__main__':
    app.run(debug=True)
