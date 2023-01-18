{{ config(materialized='table') }}

SELECT
  EXTRACT(MONTH FROM tpep_pickup_datetime) as month,
  EXTRACT(YEAR FROM tpep_pickup_datetime) as year,
  count(*) as number_of_trips
FROM
   {{ref('stg_trips')}}
GROUP BY
  1,2
ORDER BY
  2 DESC,
  1 DESC