-- guide2kulchur database setup:
    -- postgres v17
-- top level schema:
    -- relations
        -- books: Goodreads book data; includes data like descriptions, ratings distributions, and publish date
        -- users: Goodreads user data; includes data like favorite quotes, favorite genres, and list of friends
        -- authors: Goodreads author data; includes data like birth date, list of influences, and top genres
    -- notes:
        -- 1. foreign keys won't be used here, as, for example, having the requirement that the author_id field in
        --      the book relation correspond to an author tuple in authors would be untenable, unless I were to 
        --      sequentially run author, then the author's books; no foreign keys simplifies the data-collection process

CREATE TABLE IF NOT EXISTS books (
    id VARCHAR(20) PRIMARY KEY,
    url TEXT NOT NULL, 
    entered TIMESTAMP,
    title VARCHAR(100),
    author_id VARCHAR(20),
    author VARCHAR(100),
    isbn VARCHAR(13),
    language VARCHAR(20),
    image_url VARCHAR(100),
    description TEXT,
    rating REAL,
    rating_distribution JSONB,
    rating_count INT,
    review_count INT,
    top_genres TEXT[],
    currently_reading INT,
    want_to_read INT, 
    page_length INT,
    first_published DATE
);

CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(20) PRIMARY KEY,
    url TEXT NOT NULL,
    entered TIMESTAMP,
    name VARCHAR(50),
    rating REAL,
    rating_count INT,
    review_count INT,
    favorite_genres TEXT[],
    currently_reading_sample JSONB,
    quotes_sample JSONB,
    follower_count INT,
    friend_count INT,
    friends_sample JSONB,
    followings_sample JSONB
);

CREATE TABLE IF NOT EXISTS authors (
    id VARCHAR(20) PRIMARY KEY,
    url TEXT NOT NULL,
    entered TIMESTAMP,
    name TEXT,
    image_url TEXT,
    birth_place TEXT,
    birth DATE,
    death DATE,
    top_genres TEXT[],
    rating REAL,
    rating_count INT,
    review_count INT,
    follower_count INT,
    influences JSONB,
    sample_books JSONB
);
