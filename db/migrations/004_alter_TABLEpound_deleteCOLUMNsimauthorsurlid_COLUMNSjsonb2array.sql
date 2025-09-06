-- RAN:
    -- Fri Sep 5 2025

-- 1. drop sim_author_url_id
-- 2. change all jsonb columns in pound to text[]
-- reason for change:
    -- I learned that sim_author_url_id is the same as author ID
    -- simplifies the process; I still get all the value/data with just ID arrays
    -- I can just have array of ID strings, and merge for other data if needed

ALTER TABLE pound
    DROP COLUMN sim_author_url_id,
    DROP COLUMN influences,
    DROP COLUMN book_sample,
    DROP COLUMN sim_authors;

ALTER TABLE pound
    ADD COLUMN influences text[],
    ADD COLUMN book_sample text[],
    ADD COLUMN sim_authors text[];