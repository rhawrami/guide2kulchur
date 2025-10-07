-- RAN:
    -- XX Oct XX 2025

-- mass update of false_dmitry table
-- this should be done prior to ANY inserts on false_dmitry
-- reason for change:
    -- the og version of false_dmitry table is fairly simple
    -- and many of the current attrs can be changed from jsonb to array
    -- this is similar to the changes I already made to alexandria and pound
    -- I was also able to add some extra attrs since first drafting the schema

ALTER TABLE false_dmitry
    DROP COLUMN currently_reading_sample,
    DROP COLUMN friends_sample,
    DROP COLUMN followings_sample;

ALTER TABLE false_dmitry
    -- new columns first
    ADD COLUMN currently_reading_sample_books text[],
    ADD COLUMN currently_reading_sample_authors text[],
    ADD COLUMN featured_shelf_sample_books text[],
    ADD COLUMN shelf_names text[],
    ADD COLUMN followings_sample_users text[],
    ADD COLUMN followings_sample_authors text[],
    ADD COLUMN quotes_sample_strings text[],
    ADD COLUMN quotes_sample_author_ids text[],
    -- update prior dropped cols
    ADD COLUMN friends_sample text[];