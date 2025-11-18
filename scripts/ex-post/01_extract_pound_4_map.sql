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
            WHEN pound.birth IS NOT NULL THEN pound.birth::text || ' 00:00' 
            ELSE NULL
        END AS "birth date",
        pound.birth AS "Birth Date",
        regexp_replace(regexp_replace(pound.top_genres[:3]::text, '\{|\}|"', '', 'g'), ',', ', ', 'g') AS "Top Genres",
        '"' || alexandria.title || '"' AS "Most Rated Work",
        pound.rating AS "Avg. Rating",
        pound.rating_count AS "# Ratings",
        s4p_merged.sim2_merged AS "Similar to",
        'https://goodreads.com/author/show/' || pound.author_id AS "GR Link",
        birth_place_locs.lat AS "jitter_lat",
        birth_place_locs.lon AS "jitter_lon"
    FROM 
        pound
    INNER JOIN
        birth_place_locs ON lower(pound.birth_place) = birth_place_locs.og_loc
    INNER JOIN
        alexandria ON pound.book_sample[1] = alexandria.book_id
    LEFT JOIN
        s4p_merged ON pound.author_id = s4p_merged.a_id
    WHERE
        pound.top_genres[1] IS NOT NULL
    AND
        pound.rating_count >= 500
)

TO 'pound_for_kepler.csv' WITH (format csv, header);