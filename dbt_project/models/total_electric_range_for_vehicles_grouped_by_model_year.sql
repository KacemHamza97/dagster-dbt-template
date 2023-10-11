SELECT Model_Year,
    SUM(Electric_Range) AS Total_Electric_Range
FROM {{ source('main', 'vehicle_information') }}
GROUP BY Model_Year
ORDER BY Model_Year