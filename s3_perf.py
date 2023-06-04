import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import s3fs
from contexttimer import Timer
fs = s3fs.S3FileSystem(anon=True)

COLS = [None, 'dropoff_at', 'vendor_id', 'passenger_count', 'pickup_at']
PREDICATES = [
    ds.field("passenger_count") > 10, # only 1 row groups
    ds.field("passenger_count") > 5, # several row groups
    ds.field("vendor_id") == 'DDS',
    None,
]

for rawpath in [
    'nyc-taxi-test/row_groups_1.parquet',
    'nyc-taxi-test/row_groups_10.parquet',
    'nyc-taxi-test/row_groups_216.parquet',
    'ursa-labs-taxi-data/2009/01/data.parquet',
    ]:
    path = f's3://{rawpath}'
    parquet_file = pq.ParquetFile(path, filesystem=fs)
    meta = parquet_file.metadata
    print(path, meta.num_columns, ' cols', meta.num_rows, ' rows: ', meta.num_row_groups, ' rowgroups')
    
    for col in COLS:
        for pred in PREDICATES:
            with Timer() as t:
                with fs.open(rawpath, 'rb') as f:
                    df = pd.read_parquet(f,
                                         columns=[col] if col else None,
                                         filters=pred
                                         )
        print('col: ', col, ' predicate: ', pred, ' time: ', t)

        
"""
  optional binary field_id=-1 vendor_id (String);
  optional int96 field_id=-1 pickup_at;
  optional int96 field_id=-1 dropoff_at;
  optional int32 field_id=-1 passenger_count (Int(bitWidth=8, isSigned=true));
  optional float field_id=-1 trip_distance;
  optional float field_id=-1 pickup_longitude;
  optional float field_id=-1 pickup_latitude;
  optional int32 field_id=-1 rate_code_id (Null);
  optional binary field_id=-1 store_and_fwd_flag (String);
  optional float field_id=-1 dropoff_longitude;
  optional float field_id=-1 dropoff_latitude;
  optional binary field_id=-1 payment_type (String);
  optional float field_id=-1 fare_amount;
  optional float field_id=-1 extra;
  optional float field_id=-1 mta_tax;
  optional float field_id=-1 tip_amount;
  optional float field_id=-1 tolls_amount;
  optional float field_id=-1 total_amount;
"""  

with fs.open(rawpath, 'rb') as f:
    df = pd.read_parquet(f,
                     #columns=[col] if col else None,
                     filters=ds.field("passenger_count") > 5                     
                     )
