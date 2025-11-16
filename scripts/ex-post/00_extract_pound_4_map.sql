\copy (
    SELECT
        pound.img_url AS "<img>",
        pound.author_name AS "Name",
        CASE
            WHEN pound.birth IS NOT NULL THEN pound.birth::text || ' 00:00' 
            ELSE NULL
        END AS birth_for_kepler,
        SUBSTRING(pound.descr FOR 200) || '...' AS "GR Bio",
        pound.birth_place AS "Birth Place",
        pound.birth AS "Birth Date",
        regexp_replace(regexp_replace(pound.top_genres[:3]::text, '\{|\}|"', '', 'g'), ',', ', ') AS "Top Genres (3)",
        pound.rating AS "GR Rating",
        pound.rating_count AS "GR Rating Count",
        pound.review_count AS "GR Review Count",
        pound.follower_count AS "GR Follower Count",
        'https://goodreads.com/author/show/' || pound.author_id AS "GR Link",
        birth_place_locs.lat AS "jitter_lat",
        birth_place_locs.lon AS "jitter_lon"
    FROM 
        pound
    INNER JOIN
        birth_place_locs ON lower(pound.birth_place) = birth_place_locs.og_loc
    WHERE
        pound.top_genres[1] IS NOT NULL
    AND
        pound.rating_count >= 500
)

TO 'pound_for_kepler.csv' WITH (format csv, header);