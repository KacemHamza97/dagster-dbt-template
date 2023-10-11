SELECT Make, Model_Year, AVG(Base_MSRP) AS Avg_Base_MSRP
FROM {{ source('main', 'vehicle_information') }}
GROUP BY Make, Model_Year
ORDER BY Make, Model_Year
