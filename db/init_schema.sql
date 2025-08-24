/* 
guide2kulchur database setup:
    - ran on postgres v17

    - used to collect PUBLIC Goodreads book, author, and user data

    - data will first be collected from Goodreads site, then loaded into db in batches

    - after initial data is pulled, a second run will occur, filling in the sim_books and sim_authors
      attributes; the reason for this is that collecting the main book/author data AND the sim_books/sim_authors
      data requires two requests, one to the main page and one to the similar books/authors page. In order to speed
      up the initial collecting process as fast as possible, similar book/author data will be collected later. Thus,
      on initial insertion, the similar_books/similar_authors attribute will be NULL.
    
    - given the nature of this data collection, foreign keys will not be enforced, as there's no guarantee in the 
      collection process that, for example, a book's author will be in the author table at the time of insertion.

    - further, indices will not be created until after the initial data collection, again to speed up the insertion process.
*/

-- domains
CREATE DOMAIN object_rating AS real
    CONSTRAINT val_in_five CHECK (VALUE >= 1 AND VALUE <= 5);


CREATE DOMAIN pos_int AS int
    CONSTRAINT int_is_positive CHECK (VALUE >= 0);


-- functions
CREATE FUNCTION lang_to_tsvector(txt TEXT DEFAULT NULL, lang TEXT DEFAULT 'english') RETURNS tsvector AS $$
BEGIN
    RETURN to_tsvector(
        CASE
            WHEN LOWER(lang) = ANY(ARRAY['english','spanish','french',
                                         'german','italian','russian']) 
            THEN LOWER(lang)::regconfig
            ELSE 'simple'::regconfig
        END,
        COALESCE(txt,'')
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE FUNCTION updating_update_times() RETURNS TRIGGER as $$
BEGIN
    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    
    NEW.updated_at := current_timestamp;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- tables
CREATE TABLE alexandria (
    book_id text PRIMARY KEY,
    title text NOT NULL,
    author text NOT NULL,
    author_id text,
    isbn text,
    lang text,
    descr text,
    descr_vector tsvector GENERATED ALWAYS AS (lang_to_tsvector(descr, lang)) STORED,
    img_url text,
    rating object_rating,
    rating_dist jsonb,
    rating_count pos_int,
    review_count pos_int,
    top_genres text[], 
    currently_reading pos_int,
    want_to_read pos_int,
    first_published date,
    page_length pos_int,
    sim_books_url_id text,
    sim_books jsonb,    -- will be filled after initial genre pulls
    entered_at timestamptz DEFAULT current_timestamp(2),
    updated_at timestamptz DEFAULT current_timestamp(2)
);


-- author / alias := 'pound'
CREATE TABLE pound (
    author_id text PRIMARY KEY,
    author_name text NOT NULL,
    descr text,
    descr_vector tsvector GENERATED ALWAYS AS (to_tsvector('english',descr)) STORED, -- won't always be optimal, but mostly fine
    img_url text,
    birth_place text,
    birth date,
    death date,
    top_genres text[],
    influences jsonb,
    book_sample jsonb,
    quotes_sample text[],
    rating object_rating,
    rating_count pos_int,
    review_count pos_int,
    follower_count pos_int,
    sim_author_url_id text,
    sim_authors jsonb,  -- will be filled after initial genre pulls
    entered_at timestamptz DEFAULT current_timestamp(2),
    updated_at timestamptz DEFAULT current_timestamp(2)
);


-- user / alias := 'false_dmitry'
CREATE TABLE false_dmitry (
    user_id text PRIMARY KEY,
    user_name text NOT NULL,
    img_url text,
    rating object_rating,
    rating_count pos_int,
    review_count pos_int,
    favorite_genres text[],
    currently_reading_sample jsonb,
    follower_count pos_int,
    friend_count pos_int,
    friends_sample jsonb,
    followings_sample jsonb,
    entered_at timestamptz DEFAULT current_timestamp(2),
    updated_at timestamptz DEFAULT current_timestamp(2)
);


-- triggers
CREATE TRIGGER update_time_alx 
BEFORE UPDATE ON alexandria
    FOR EACH ROW EXECUTE FUNCTION updating_update_times();


CREATE TRIGGER update_time_pnd 
BEFORE UPDATE ON pound
    FOR EACH ROW EXECUTE FUNCTION updating_update_times();


CREATE TRIGGER update_time_dmtry 
BEFORE UPDATE ON false_dmitry
    FOR EACH ROW EXECUTE FUNCTION updating_update_times();