{{ config(materialized='table') }}

SELECT
    LocationID,
    Borough,
    Zone,
    service_zone
FROM
    `tribal-logic-361320.taxi_data.zones_data` 