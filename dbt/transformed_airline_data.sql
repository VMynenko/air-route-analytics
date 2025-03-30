{{ config(
    materialized='table'
) }}

WITH airline_data AS (
    SELECT  
        Year AS year,
        city1 AS dep_city,
        city2 AS arr_city,
        airport_1 AS dep_airport,
        airport_2 AS arr_airport,
        fare
    FROM {{ source('de_zoomcamp', 'us_airline_data') }}
),

final AS (
    SELECT  
        year,
        dep_city,
        arr_city,
        dep_airport,
        arr_airport,
        AVG(fare) AS avg_ticket_price
    FROM airline_data
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM final
WHERE year >= 1996
