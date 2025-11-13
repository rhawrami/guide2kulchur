-- RAN:
    -- Tue Nov 12 2025

-- make table for storing author locations,
-- based on the birth_place column

-- Using Nominatim for addr/lat/lon
-- Location string might not actually map to 
    -- Nominatim prediction (or Nominatim may not find anything)

CREATE TABLE IF NOT EXISTS birth_place_locs (
    og_loc text PRIMARY KEY,
    addr text,
    lat real,
    lon real
);