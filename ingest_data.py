import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.csv"

    # download the csv
    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name,
                          compression='gzip',
                          iterator=True,
                          chunksize=100000

    )
    # df = next(df_iter)
    # df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    for item in df_iter:
        t_start = time()
        item.tpep_pickup_datetime = pd.to_datetime(item.tpep_pickup_datetime)
        item.tpep_dropoff_datetime = pd.to_datetime(item.tpep_dropoff_datetime)
        item.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f'Inserted another chunk..., took {(t_end - t_start):.3f} seconds')
        
    print('Job done!')    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)