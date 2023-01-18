{{ config(materialized='table') }}

SELECT
  CASE 
    WHEN t.VendorID = 1 THEN 'Creative Mobile Technologies'
    WHEN t.VendorID = 2 THEN 'VeriFone Inc.'
    WHEN t.VendorID = 0 THEN 'Unknown'
    ELSE NULL
  END AS vendor,
  t.tpep_pickup_datetime as pickup_datetime, 
  t.tpep_dropoff_datetime as dropoff_datetime,
  passenger_count as number_of_passengers,
  trip_distance,
  zp.Borough as pickup_district,
  zp.Zone as pickup_district_zone,
  zd.Borough as dropoff_district,
  zd.Zone as dropoff_district_zone,
  CASE
    WHEN t.payment_type = 1 THEN 'Credit card'
    WHEN t.payment_type = 2 THEN 'Cash'
    WHEN t.payment_type = 3 THEN 'No charge'
    WHEN t.payment_type = 4 THEN 'Dispute'
    WHEN (t.payment_type = 5 OR t.payment_type = 0) THEN 'Unknown'
    WHEN t.payment_type = 6 THEN 'Voided trip'
  END AS payment_type,
  total_amount 

FROM
  {{ref('stg_trips')}} t
  INNER JOIN {{ref('stg_zones')}} zp on t.PULocationID = zp.LocationID 
  INNER JOIN {{ref('stg_zones')}}  zd on t.DOLocationID = zd.LocationID
