-- RAN:
    -- Mon Aug 25 2025

-- change sim_books column in alexandria from jsonb to text[]
-- reason for change:
    -- I don't need to have a list of ID/title/author dicts anymore
    -- I can just have array of ID strings, and merge for other data if needed

ALTER TABLE alexandria
    DROP COLUMN sim_books;

ALTER TABLE alexandria
    ADD COLUMN sim_books text[];