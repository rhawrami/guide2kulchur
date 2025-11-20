-- RAN:
    -- WED Nov 19 2025

-- make table for storing books that won in the annual Goodreads Book Awards
-- just need to store the book and author ID, award year, award category and number of award votes

CREATE TABLE IF NOT EXISTS gr_awards (
    book_id text,
    author_id text,
    award_year int,
    award_category text,
    award_num_votes text
)