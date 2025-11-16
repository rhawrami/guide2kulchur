\copy (
    SELECT
        alx.title AS "Title",
        alx.author AS "Author",
        alx.first_published::text || ' 00:00' AS first_pub_for_kepler,
        alx.first_published AS "First Published",
        alx.rating AS "Avg. Rating",
        alx.rating_count AS "# Ratings",
        alx.review_count AS "# Reviews",
        SUBSTRING(alx.descr FOR 200) || '...' AS "GR Descr.",
        'https://goodreads.com/book/show/' || alx.book_id AS "GR Link",
        bpl.lat AS "lat",
        bpl.lon AS "lon"
    FROM
        alexandria alx
    JOIN
        pound pnd ON alx.author_id = pnd.author_id
    JOIN
        birth_place_locs bpl ON lower(pnd.birth_place) = bpl.og_loc
    WHERE
        alx.rating_count >= 1000
    AND
        alx.first_published IS NOT NULL
    AND
        alx.first_published < '2026-01-01'
)

TO 'alexandria_for_kepler.csv' WITH (format csv, header);