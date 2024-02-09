import datetime

import pandas as pd
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # drop any rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    data = data.query("passenger_count != 0 and trip_distance != 0")

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id
    renamed = 0
    
    def rename_col(col: str):
        nonlocal renamed
        if (not col.__contains__('_')):
            parts = re.findall('..?', col)
            coln = ''
            for part in parts: 
                if (part == part.upper()): coln = coln + part.capitalize() 
                else: coln = coln + part
            res = ''.join('_' + c.lower() if c.isupper() else c for c in coln).lstrip('_')
            if (res != col): renamed = renamed + 1
            return res 
        else:
            return col
    
    for col in data.columns:
        data.rename( columns={ col: rename_col(col) }, inplace=True)

    # print how many cloumns were renamed
    print(f'colums renamed: {renamed}')

    # check for values if vedndor_id column
    print(data['vendor_id'].unique())

    return data


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is not as expected'
    assert 'vendor_id' in output.columns, 'The output is not as expected'
    assert output.query("passenger_count == 0").shape[0] == 0
    assert output.query("trip_distance == 0").shape[0] == 0
