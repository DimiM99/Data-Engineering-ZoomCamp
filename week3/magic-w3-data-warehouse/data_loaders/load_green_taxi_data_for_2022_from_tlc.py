import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    # create a list of urls to download data from
    urls = []
    for month in range(1, 13):
        month = "0" + str(month) if len(str(month)) == 1 else str(month)
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet'
        urls.append(url)

    # The list of dataframes
    dfs = []

    # Download the data for each month
    for source in urls:
        # Download the data
        response = requests.get(source)
        response.raise_for_status()
        df = pd.read_parquet(source, engine='pyarrow')
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
