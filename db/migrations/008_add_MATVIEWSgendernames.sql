-- RAN:
    -- Tue Nov 11 2025

-- add gendered-names materialized views
-- use the name_x_gender table to predict an author/user's gender

-- author genders
-- here, we use the world name gender dataset AND the pronouns included
-- in the author description. If there is a mismatch in predictions,
-- then the author description prediction takes priority.
CREATE MATERIALIZED VIEW g_pound AS
    WITH pound_pronoun_counts(author_id, author_name, c_female, c_male) AS (
        SELECT 
            author_id,
            author_name,
            array_length(
                regexp_split_to_array(
                    regexp_replace(lower(descr), '\.? (she|her)( |\.|;|,)', '992849', 'g'), '992849'
                ), 1
            ) - 1,
            array_length(
                regexp_split_to_array(
                    regexp_replace(lower(descr), '\.? h(e|is)( |\.|;|,)', '992849', 'g'), '992849'
                ), 1
            ) - 1
        FROM 
            pound
    )
    SELECT
        author_id,
        author_name,
        c_male AS pronoun_count_male,
        c_female AS pronoun_count_female,
        CASE
            -- good when same prediction
            WHEN (c_male > c_female) AND (g_gender = 'M') THEN 'M'
            WHEN (c_male < c_female) AND (g_gender = 'F') THEN 'F'
            -- equal
            WHEN (c_male = c_female) AND(g_gender IS NOT NULL) THEN g_gender
            WHEN (c_male = c_female) AND(g_gender IS NULL) THEN 'EQ'
            -- pronoun count prioritized over name joins
            WHEN (c_male > c_female) AND (g_gender = 'F') THEN 'M' 
            WHEN (c_male < c_female) AND (g_gender = 'M') THEN 'F'
            -- no other choice
            WHEN (c_male > c_female) AND (g_gender IS NULL) THEN 'M' 
            WHEN (c_male < c_female) AND (g_gender IS NULL) THEN 'F'
            -- no other choice
            WHEN (c_female IS NULL) AND (g_gender IS NOT NULL) THEN g_gender
            -- both null
            ELSE NULL
        END AS g_comp,
        CASE
            WHEN c_female IS NULL THEN NULL -- means that description is null
            WHEN c_male > c_female THEN 'M'
            WHEN c_male < c_female THEN 'F'
            ELSE 'EQ' -- 'EQ' for when equal
        END AS g_pronoun_count,
        CASE
            WHEN g_gender IS NOT NULL THEN g_gender
            ELSE NULL
        END AS g_nxg
    FROM
        pound_pronoun_counts
    LEFT JOIN 
        name_x_gender 
        ON regexp_replace(lower(author_name), '\s.*$', '') = g_name;

-- user genders
-- here, we don't have access to descriptions, so we can only rely on the
-- world name gender dataset.
CREATE MATERIALIZED VIEW g_dmitry AS
    SELECT
        user_id,
        user_name, 
        CASE
            WHEN g_gender IS NOT NULL THEN g_gender
            ELSE NULL
        END AS g_nxg
    FROM
        false_dmitry
    LEFT JOIN 
        name_x_gender 
        ON regexp_replace(lower(user_name), '\s.*$', '') = g_name;