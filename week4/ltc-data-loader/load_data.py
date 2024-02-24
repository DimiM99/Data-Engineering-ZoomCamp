# this script download the tlc website for yellow/green taxi and fhv data and saves them in respective pq files.

import warnings

import pandas as pd
import requests
from pandas import DataFrame

warnings.filterwarnings('ignore')

needed_data = [
    {'type': 'green', 'years': [2019, 2020]},
    {'type': 'yellow', 'years': [2019, 2020]},
    {'type': 'fhv', 'years': [2019]}
]


def download_data():
    g_df: DataFrame = pd.DataFrame()
    y_df: DataFrame = pd.DataFrame()
    f_df: DataFrame = pd.DataFrame()
    for collection in needed_data:
        trip_type = collection['type']
        for year in collection['years']:
            for month in range(1, 13):
                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{trip_type}_tripdata_{year}-{month:02d}.parquet'
                print(f"Downloading {trip_type} trip data for {year}-{month:02d}")
                response = requests.get(url)
                response.raise_for_status()
                df = pd.read_parquet(url, engine='pyarrow')
                match trip_type:
                    case 'green':
                        g_df = pd.concat([g_df, df], ignore_index=True)
                    case 'yellow':
                        y_df = pd.concat([y_df, df], ignore_index=True)
                    case 'fhv':
                        f_df = pd.concat([f_df, df], ignore_index=True)
        match trip_type:
            case 'green':
                yield g_df, trip_type
                g_df = pd.DataFrame()
            case 'yellow':
                yield y_df, trip_type
                y_df = pd.DataFrame()
            case 'fhv':
                yield f_df, trip_type
                f_df = pd.DataFrame()


def save_data(df: DataFrame, trip_type: str):
    print(f'Saving {trip_type} trip data to parquet file')
    match trip_type:
        case 'green':
            df.to_parquet('data/green_taxi_data.parquet', engine='pyarrow')
        case 'yellow':
            df.to_parquet('data/yellow_taxi_data.parquet', engine='pyarrow')
        case 'fhv':
            df.to_parquet('data/fhv_data.parquet', engine='pyarrow')


def main():
    print('Downloading data...')
    for data, trip_type in download_data():
        save_data(data, trip_type)
    print('Data downloaded and saved successfully')


if __name__ == '__main__':
    main()
