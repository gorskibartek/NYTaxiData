{{ config(materialized='table') }}

SELECT
  EXTRACT(MONTH FROM tpep_pickup_datetime) as month,
  EXTRACT(YEAR FROM tpep_pickup_datetime) as year,
  SUM(passenger_count) as number_of_passengers
FROM
  `tribal-logic-361320.taxi_data.raw_data` 
GROUP BY
  1,2