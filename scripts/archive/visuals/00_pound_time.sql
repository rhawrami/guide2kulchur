-- The query/command below extracts author records with existing
-- birth date and birth place data. We first find two authors that each
-- author is similar to (if the sim_authors field is non-null), then merge this
-- with pound, along with the birth place data. Further, we restrict authors to
-- those with rating counts above 100.

-- extract:
        -- img (for author image on popup)
        -- name
        -- transformend birth date (for authors born in BC, must -> '0001-01-01 for kepler to recognize as time column')
        -- recorded birthdate 
        -- top genres (only show 3)
        -- example work
        -- rating
        -- rating_count (will be used for marker sizing)
        -- similar authors
        -- Goodreads author page link
        -- jitter latitude/longitude (jitter as many authors will have the same recorded birth place)
\copy (
    WITH s4p_1(a_id, s_name) AS (
        SELECT 
            p1.author_id,
            p2.author_name
        FROM
            pound p1
        INNER JOIN
            pound p2
        ON
            p1.sim_authors[1] = p2.author_id
        WHERE
            p1.sim_authors[1] IS NOT NULL
    ),
    s4p_2(a_id, s_name) AS (
        SELECT 
            p1.author_id,
            p2.author_name
        FROM
            pound p1
        INNER JOIN
            pound p2
        ON
            p1.sim_authors[2] = p2.author_id
        WHERE
            p1.sim_authors[2] IS NOT NULL
    ),
    s4p_merged(a_id, sim2_merged) AS (
        SELECT
            s4p_1.a_id,
            CASE
                WHEN s4p_2.s_name IS NOT NULL THEN s4p_1.s_name || ', ' || s4p_2.s_name
                ELSE s4p_1.s_name
            END AS sim2_merged
        FROM
            s4p_1
        LEFT JOIN
            s4p_2 ON s4p_1.a_id = s4p_2.a_id
    )
    SELECT
        pound.img_url AS "<img>",
        pound.author_name AS "Name",
        CASE
            WHEN pound.birth IS NOT NULL AND pound.birth < '01/01/0001' THEN '01/01/0001'::date::text || ' 00:00' 
            WHEN pound.birth IS NOT NULL AND pound.birth >= '01/01/0001' THEN pound.birth::text || ' 00:00' 
            ELSE NULL
        END AS "birth date",
        pound.birth AS "Birth Date",
        regexp_replace(regexp_replace(pound.top_genres[:3]::text, '\{|\}|"', '', 'g'), ',', ', ', 'g') AS "Top Genres",
        '"' || alexandria.title || '"' AS "Example Work",
        pound.rating AS "Avg. Rating",
        pound.rating_count AS "# Ratings",
        s4p_merged.sim2_merged AS "Similar to",
        'https://goodreads.com/author/show/' || pound.author_id AS "GR Link",
        CASE 
            WHEN pound.rating_count % 2 = 0 THEN birth_place_locs.lat + random() * 0.25
            ELSE birth_place_locs.lat - random() * 0.25
        END AS "jitter_lat",
        CASE 
            WHEN pound.rating_count % 2 = 0 THEN birth_place_locs.lon + random() * 0.25
            ELSE birth_place_locs.lon - random() * 0.25
        END AS "jitter_lon"
    FROM 
        pound
    INNER JOIN
        birth_place_locs ON lower(pound.birth_place) = birth_place_locs.og_loc
    LEFT JOIN
        alexandria ON pound.book_sample[1] = alexandria.book_id
    LEFT JOIN
        s4p_merged ON pound.author_id = s4p_merged.a_id
    WHERE
        pound.birth IS NOT NULL
    AND
        pound.top_genres[1] IS NOT NULL
    AND
        pound.rating_count >= 100
)

TO 'visualizations/maps/pnd_time_trends_landing.csv' WITH (format csv, header);