{{ config(materialized='table') }}

SELECT
  z.Borough as pickup_district,
  count(t.tpep_pickup_datetime) as number_of_trips
FROM
  {{ref('stg_zones')}} z
  INNER JOIN {{ref('stg_trips')}} t ON t.PULocationID = z.LocationID
GROUP BY
  1