{{ config(materialized='table') }}
with raw_data as (
SELECT  
    VendorID, 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    payment_type,
    total_amount,
    PULocationID,
    DOLocationID,
    CAST(SUBSTR(filename, 17,4) as INTEGER) as year_from_file,
    CAST(SUBSTR(filename, 22,2) as integer) as month_from_file
FROM
  `tribal-logic-361320.taxi_data.raw_data` 
)
SELECT  
  *
FROM  
  raw_data
WHERE
  EXTRACT(YEAR from tpep_dropoff_datetime) = year_from_file
  AND EXTRACT(MONTH from tpep_dropoff_datetime) = month_from_file
  