import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import s3fs
from contexttimer import Timer
fs = s3fs.S3FileSystem(anon=True)

COLS = [None, 'dropoff_at', 'vendor_id', 'passenger_count', 'pickup_at']
PREDICATES = [
    (ds.field("vendor_id") == 'DDS') & (ds.field("passenger_count") > 5),
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
            print('col: ', col, ', predicate: ', pred, ', time: ', t)

        
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

# with fs.open(rawpath, 'rb') as f:
#     df = pd.read_parquet(f,
#                      #columns=[col] if col else None,
#                      filters=ds.field("passenger_count") > 5                     
#                      )
"""
s3://nyc-taxi-test/row_groups_10.parquet 18  cols 14092413  rows:  11  rowgroups
col:  None  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  5.263
col:  None  predicate:  (passenger_count > 10)  time:  0.575
col:  None  predicate:  (passenger_count > 5)  time:  4.956
col:  None  predicate:  (vendor_id == "DDS")  time:  5.602
col:  None  predicate:  None  time:  10.377
col:  dropoff_at  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  3.017
col:  dropoff_at  predicate:  (passenger_count > 10)  time:  0.226
col:  dropoff_at  predicate:  (passenger_count > 5)  time:  1.812
col:  dropoff_at  predicate:  (vendor_id == "DDS")  time:  2.402
col:  dropoff_at  predicate:  None  time:  1.879
col:  vendor_id  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  1.444
col:  vendor_id  predicate:  (passenger_count > 10)  time:  0.183
col:  vendor_id  predicate:  (passenger_count > 5)  time:  1.464
col:  vendor_id  predicate:  (vendor_id == "DDS")  time:  1.022
col:  vendor_id  predicate:  None  time:  2.593
col:  passenger_count  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  1.782
col:  passenger_count  predicate:  (passenger_count > 10)  time:  0.105
col:  passenger_count  predicate:  (passenger_count > 5)  time:  0.700
col:  passenger_count  predicate:  (vendor_id == "DDS")  time:  1.474
col:  passenger_count  predicate:  None  time:  0.723
col:  pickup_at  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  2.747
col:  pickup_at  predicate:  (passenger_count > 10)  time:  0.321
col:  pickup_at  predicate:  (passenger_count > 5)  time:  2.611
col:  pickup_at  predicate:  (vendor_id == "DDS")  time:  1.666
col:  pickup_at  predicate:  None  time:  1.826
s3://nyc-taxi-test/row_groups_216.parquet 18  cols 14092413  rows:  216  rowgroups
col:  None  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  5.732
col:  None  predicate:  (passenger_count > 10)  time:  0.153
col:  None  predicate:  (passenger_count > 5)  time:  5.762
col:  None  predicate:  (vendor_id == "DDS")  time:  5.795
col:  None  predicate:  None  time:  6.407
col:  dropoff_at  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  16.918
col:  dropoff_at  predicate:  (passenger_count > 10)  time:  0.167
col:  dropoff_at  predicate:  (passenger_count > 5)  time:  7.662
col:  dropoff_at  predicate:  (vendor_id == "DDS")  time:  10.774
col:  dropoff_at  predicate:  None  time:  9.577
col:  vendor_id  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  18.118
col:  vendor_id  predicate:  (passenger_count > 10)  time:  0.121
col:  vendor_id  predicate:  (passenger_count > 5)  time:  13.162
col:  vendor_id  predicate:  (vendor_id == "DDS")  time:  9.664
col:  vendor_id  predicate:  None  time:  9.685
col:  passenger_count  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  13.922
col:  passenger_count  predicate:  (passenger_count > 10)  time:  0.129
col:  passenger_count  predicate:  (passenger_count > 5)  time:  9.045
col:  passenger_count  predicate:  (vendor_id == "DDS")  time:  15.515
col:  passenger_count  predicate:  None  time:  8.288
col:  pickup_at  predicate:  ((vendor_id == "DDS") and (passenger_count > 5))  time:  14.552
col:  pickup_at  predicate:  (passenger_count > 10)  time:  0.128
col:  pickup_at  predicate:  (passenger_count > 5)  time:  17.097
col:  pickup_at  predicate:  (vendor_id == "DDS")  time:  7.991
col:  pickup_at  predicate:  None  time:  8.735
"""
