WITH film_counts AS (
    SELECT fcat.category_id,
        COUNT(*) AS film_count
    FROM {{ source('main', 'film_category') }} AS fcat
    GROUP BY fcat.category_id
)
SELECT fc.category_id,
    c.name AS category_name,
    fc.film_count
FROM film_counts fc
    JOIN {{ source('main', 'category') }} AS c ON fc.category_id = c.category_id
ORDER BY fc.film_count DESC
LIMIT 5