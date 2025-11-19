-- RAN:
    -- Tue Nov 18 2025

-- make table for storing author birth/death dates and birth locations
-- data from WikiData (super awesome service btw)
-- link: https://www.wikidata.org/wiki/Wikidata:Main_Page

CREATE TABLE IF NOT EXISTS wikidata_lb (
    author_code text PRIMARY KEY,
    author_lab text,
    native_name text,
    occupation_lab text,
    dob date,
    dod date,
    pob text,
    pob_lab text
);