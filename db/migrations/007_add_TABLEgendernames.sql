-- RAN:
    -- Tue Nov 11 2025

-- add gendered-names table
-- has a name and corresponding gender
-- data comes from the World Gender Name Dictionary 2.0 Dataset
    -- link here: https://tind.wipo.int/record/49408?v=tab&ln=en
    -- specifically: wgnd_2_0_name-gender_nocode.csv
        -- using this set as we don't have reliable locations for most authors/users
-- once the table is loaded, we can insert the data through a copy

CREATE TABLE IF NOT EXISTS name_x_gender (
    g_name text PRIMARY KEY,    -- there shouldn't be any duplicates
    g_gender text
);

-- now, assuming you have wgnd_2_0_name-gender_nocode.csv named as "n_x_g.csv":
    -- \copy name_x_gender FROM 'n_x_g.csv' WITH DELIMITER ',' CSV HEADER;