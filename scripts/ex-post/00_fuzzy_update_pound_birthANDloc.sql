-- UPDATE: set births after 2020 to null
-- i could choose 2015 or 2017 or ..., but 2020 makes sense.
UPDATE
    pound
SET 
    birth = NULL
WHERE 
    birth >= '2020-01-01';


-- UPDATE: find birth dates from "c." and/or "fl."
with cfl_null_birth(a_id, a_name, matches) AS (
    SELECT
        author_id,
        author_name,
        regexp_matches(SUBSTRING(lower(descr) FOR 100), 
        -- this regex is a mess, but it captures a wide range of birth date formats
        '[^\w]((c\.|fl\.)\s?\d{1,4}((rd|nd|th|st)\s(century|c\.))?(\s?(a\.?d\.?|b\.?c\.?|c\.?e\.?)e?\.?)?\s?(to|or|-|–)?\s?(c\.?|fl\.?)?(\s?\d{1,4}(\s?(a\.?d\.?|b\.?c\.?|c\.?e\.?)?e?\.?\s?)?)?)'
        )
    FROM 
        pound
    WHERE
        birth IS NULL
),
cfl_null_birth_replacements(a_id, a_name, new_birth) AS (
    SELECT
        a_id, 
        a_name,
        (
            CASE
                -- century, BC
                -- put them in mid-century, hence -50
                WHEN matches[1] ~ 'b\.?c\.?' AND matches[1] LIKE '%century%' 
                THEN '01/01/' || lpad((regexp_replace(matches[1], '\D', '', 'g')::int * 100 - 50)::text, 4, '0') || ' BC'
                -- century, AD
                WHEN matches[1] ~ '[^b](c\.?e\.?|a\.?d\.?)e?' AND matches[1] LIKE '%century%' 
                THEN '01/01/' || lpad((regexp_replace(matches[1], '\D', '', 'g')::int * 100 - 50)::text, 4, '0') || ' AD'
                -- non-century, BC
                WHEN matches[1] ~ 'b\.?c\.?' AND matches[1] NOT LIKE '%century%'
                THEN '01/01/' || lpad(regexp_replace(matches[1], '\s?[-–]\s?.*$|[\D\s\.]', '', 'g'), 4, '0')  || ' BC'
                -- non-century, AD
                WHEN matches[1] ~ '[^b](c\.?e\.?|a\.?d\.?)e?' AND matches[1] NOT LIKE '%century%'
                THEN '01/01/' || lpad(regexp_replace(matches[1], '\s?[-–]\s?.*$|[\D\s\.]', '', 'g'), 4, '0') || ' AD'
                -- NA BC/AD
                ELSE '01/01/' || lpad(regexp_replace(matches[1], '\s?[-–]\s?.*$|[\D\s\.]', '', 'g'), 4, '0') || ' AD'
            END
        )::date
    FROM
        cfl_null_birth
)

UPDATE
    pound
SET
    birth = br.new_birth
FROM
    cfl_null_birth_replacements br
WHERE
    pound.author_id = br.a_id;


-- UPDATE: insert implicit BC "birth" values
-- for authors without "c.|fl." in their bio
-- more hit-or-miss than compared to "c.|fl." method
WITH bc_null_birth(a_id, a_name, matches) AS (
    SELECT 
        author_id,
        author_name,
        -- assume birth date will be early on in description
        regexp_matches(SUBSTRING(descr FOR 100), '(\d{1,4}(B?\.?C?\.?E?\s?[to-–]\d{1,4})? B\.?C\.?E?)')
    FROM
        pound
    WHERE 
        birth IS NULL
),
bc_null_birth_replacements(a_id, a_name, new_birth) AS (
    SELECT 
        a_id,
        a_name,
        CASE
            WHEN matches[2] IS NOT NULL THEN ('01/01/' || regexp_replace(regexp_replace(matches[1], matches[2], ''), '[Bb].*$', 'BC'))::date
            ELSE ('01/01/' || regexp_replace(matches[1], '[Bb].*$', 'BC'))::date
        END
    FROM
        bc_null_birth
    WHERE regexp_replace(matches[1], '[Bb].*$', 'BC') !~ '0{3,4}\sBC'
)
UPDATE 
    pound
SET 
    birth = br.new_birth
FROM
    bc_null_birth_replacements br 
WHERE 
    pound.author_id = br.a_id;


-- UPDATE: insert implicit AD "birth" values
-- for authors without "c.|fl." in their bio
-- more hit-or-miss than compared to "c.|fl." method
WITH ad_null_birth(a_id, a_name, matches) AS (
    SELECT 
        author_id,
        author_name,
        -- assume birth date will be early on in description
        regexp_matches(SUBSTRING(descr FOR 100), '(\d{1,4}(A?\.?D?\.?E?\s?[to-–]\d{1,4})? A\.?D\.?[^\w])')
    FROM
        pound
    WHERE 
        birth IS NULL    
),
ad_null_birth_replacements(a_id, a_name, new_birth) AS (
    SELECT 
        a_id,
        a_name,
        CASE
            WHEN matches[2] IS NOT NULL THEN ('01/01/' || lpad(regexp_replace(regexp_replace(matches[1], matches[2], ''), '[Aa].*$', ''), 4, '0'))::date
            ELSE ('01/01/' || lpad(regexp_replace(matches[1], '[Aa].*$', ''), 4, '0'))::date
        END
    FROM
        ad_null_birth
)
UPDATE
    pound
SET
    birth = br.new_birth
FROM
    ad_null_birth_replacements br
WHERE
    pound.author_id = br.a_id
AND 
-- needed, as some modern authors have AD in their bios for other reasons
    br.new_birth < '1900-01-01'; 


-- UPDATE: simple 4-year y-to-y range
WITH simple_yr_range_match(a_id, a_name, matches) AS (
    SELECT
        author_id,
        author_name,
        regexp_matches(SUBSTRING(descr FOR 100), '(\(\d{4}\s?[-–]\s?\d{4}\))')
    FROM
        pound
    WHERE
        birth IS NULL
),
simple_yr_replacement(a_id, a_name, new_birth) AS (
    SELECT
        a_id,
        a_name,
        ('01/01/' || regexp_replace(matches[1], '[\(\)]|[-–]\s?.*$', '', 'g'))::date
    FROM
        simple_yr_range_match
)
UPDATE
    pound
SET
    birth = syr.new_birth
FROM
    simple_yr_replacement syr
WHERE
    pound.author_id = syr.a_id;


-- UPDATE: find birth year based on "born [preposition] \d{4}"
WITH born_match4(a_id, a_name, matches) AS (
    SELECT
        author_id,
        author_name,
        regexp_matches(SUBSTRING(lower(descr) FOR 200), '(born ([io]n|around) \d{4})')
    FROM
        pound
    WHERE
        birth IS NULL
),
born_match4_replacement(a_id, a_name, new_birth) AS (
    SELECT
        a_id,
        a_name, 
        ('01/01/' || regexp_replace(matches[1], '\D', '', 'g'))::date
    FROM
        born_match4
)
UPDATE
    pound
SET 
    birth = bmr.new_birth
FROM
    born_match4_replacement bmr
WHERE
    pound.author_id = bmr.a_id;


-- UPDATE: find birth year based on "born [preposition] [a-z]+ \d{2,4}"
WITH born_match(a_id, a_name, matches) AS (
    SELECT
        author_id,
        author_name,
        regexp_matches(SUBSTRING(lower(descr) FOR 200), 
        'born [io]n ([a-z]+)\.? (\d{2,4})(rd|st|th|nd)?,? (\d{4})?'
        )
    FROM
        pound
    WHERE
        birth IS NULL
    AND
        SUBSTRING(lower(descr) FOR 200) ~ '(jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec)[a-z]+\.? \d{2,4}'
),
born_match_replacement(a_id, a_name, new_birth) AS (
    SELECT
        a_id,
        a_name,
        CASE
            -- format: jan[uary]\.?[,\s]\d{2}[,\s]\d{4}
            WHEN matches[4] IS NULL THEN (matches[1] || ' 1, ' || matches[2])::date
            -- format: jan[uary]\.?[,\s]\d{4}
            WHEN (matches[4] IS NOT NULL) AND (matches[2] ~ '\d{2}') THEN (matches[1] || ' ' || matches[2] || ', ' || matches[4])::date
            -- ignore
            ELSE NULL
        END 
    FROM
        born_match
    WHERE
        NOT (length(matches[2]) = 2 AND matches[4] IS NULL)
)
UPDATE
    pound
SET 
    birth = bmr.new_birth
FROM
    born_match_replacement bmr
WHERE
    pound.author_id = bmr.a_id;
