import time
from sqlalchemy import create_engine, text

# Connection string
connection_string = (
    "mssql+pyodbc://manoharb:Arjunsuha1*@manoharb.database.windows.net:1433/manoharb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

# Create SQLAlchemy engine
engine = create_engine(connection_string)

def measure_query_time():
    start_time = time.time()
    with engine.connect() as connection:
        connection.execute(text('SELECT COUNT(*) FROM earthquakes WHERE Magnitude > 5.0'))
    end_time = time.time()
    return end_time - start_time

if __name__ == '__main__':
    print(f"Query time: {measure_query_time()} seconds")
