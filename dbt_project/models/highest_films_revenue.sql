-- models/categories_most_films_highest_revenue.sql
WITH category_film_counts AS (
    SELECT fc.category_id,
        COUNT(fc.film_id) AS film_count
    FROM {{ source('main', 'film_category') }} AS fc
        JOIN {{ source('main', 'category') }} AS c ON fc.category_id = c.category_id
    GROUP BY fc.category_id
),
category_total_revenue AS (
    SELECT fc.category_id,
        SUM(f.rental_rate) * COUNT(fc.film_id) AS total_revenue
    FROM {{ source('main', 'film_category') }} AS fc
        JOIN {{ source('main', 'film') }} AS f ON fc.film_id = f.film_id
    GROUP BY fc.category_id
)
SELECT cfc.category_id,
    cfc.film_count,
    ctr.total_revenue
FROM category_film_counts AS cfc
    JOIN category_total_revenue AS ctr ON cfc.category_id = ctr.category_id
ORDER BY cfc.film_count DESC,
    ctr.total_revenue DESC