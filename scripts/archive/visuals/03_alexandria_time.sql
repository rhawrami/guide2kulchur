
    -- U.S.:
        -- bpl.addr ~ 'United States of America$'
        -- lang = 'English'
    -- U.K.:
        -- bpl.addr ~ ', United Kingdom$'
        -- lang = 'English'
    -- Japan:
        -- bpl.addr ~ ', 日本$'
        -- lang = 'Japanese'
\copy (
    WITH ids_to_pull(b_id) AS (
        (
            SELECT
                DISTINCT unnest(book_sample)
            FROM
                pound
        )
        INTERSECT
        (
            SELECT
                (min(book_id::int))::text
            FROM
                alexandria
            WHERE 
                rating_count >= 500
            AND
                lang = 'Japanese'
            GROUP BY 
                title, 
                author_id,
                first_published
        )
    )
    SELECT
        alx.img_url AS "<img>",
        alx.title AS "Title",
        alx.author AS "Author",
        SUBSTRING(alx.descr FOR 150) || '...' AS "GR Descr.",
        alx.first_published::text || ' 00:00' AS "first published",
        alx.first_published AS "First Published",
        alx.rating AS "Avg. Rating",
        alx.rating_count AS "# Ratings",
        'https://goodreads.com/book/show/' || alx.book_id AS "GR Link",
        CASE 
            WHEN alx.rating_count % 2 = 0 THEN bpl.lat + random() * 0.10
            ELSE bpl.lat - random() * 0.10
        END AS "jitter_lat",
        CASE 
            WHEN alx.rating_count % 2 = 0 THEN bpl.lon + random() * 0.10
            ELSE bpl.lon - random() * 0.10
        END AS "jitter_lon"
    FROM
        alexandria alx
    INNER JOIN
        ids_to_pull ON alx.book_id = ids_to_pull.b_id
    INNER JOIN
        pound pnd ON alx.author_id = pnd.author_id
    INNER JOIN
        birth_place_locs bpl ON lower(pnd.birth_place) = bpl.og_loc
    WHERE
        alx.first_published IS NOT NULL
    AND
        alx.top_genres[1] IS NOT NULL
    AND
        alx.descr IS NOT NULL
    AND
        bpl.addr ~ ', 日本$'
    AND
        alx.first_published < '2025-11-01'
    AND 
        alx.first_published > '1499-12-31'
)
TO 'visualizations/maps/alx_time_trends_JP.csv' WITH (format csv, header);

