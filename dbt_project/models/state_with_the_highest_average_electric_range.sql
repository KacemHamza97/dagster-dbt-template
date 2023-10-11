WITH StateElectricRange AS (
    SELECT l.State, AVG(v.Electric_Range) AS Avg_Electric_Range
    FROM {{ source('main', 'location_information') }} l
    JOIN {{ source('main', 'vehicle_information') }} v using(id)
    GROUP BY l.State
)    
SELECT State   
FROM StateElectricRange    
WHERE Avg_Electric_Range = (SELECT MAX(Avg_Electric_Range) FROM StateElectricRange)   
    