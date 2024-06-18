import pandas as pd
from sqlalchemy import create_engine, text

# Connection string
connection_string = (
    "mssql+pyodbc://manoharb:Arjunsuha1*@manoharb.database.windows.net:1433/manoharb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create SQLAlchemy engine
engine = create_engine(connection_string)

def import_data(csv_file):
    df = pd.read_csv(csv_file)
    with engine.connect() as connection:
        for index, row in df.iterrows():
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

if __name__ == '__main__':
    import_data('src/earthquakes.csv')
