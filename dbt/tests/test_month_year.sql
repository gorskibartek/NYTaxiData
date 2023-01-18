with trips_data as (
    SELECT
        *
    FROM
        {{ref('stg_trips')}}
)
SELECT  
    tpep_dropoff_datetime
FROM   
    trips_data
WHERE
    EXTRACT(YEAR from tpep_dropoff_datetime) != year_from_file
    AND EXTRACT(MONTH from tpep_dropoff_datetime) != month_from_file