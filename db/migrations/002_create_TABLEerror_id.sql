-- RAN:
    -- Mon Aug 29 2025

-- add table to track error IDs during collection process: error_ids
-- reason for change:
    -- using the sim_books column in alexandria, I can keep pulling new books based on their IDs
    -- but if the ID is not actually valid, then it's just a wasted attempt
    -- here, we can track those IDs that we pulled and learned where invalid, so we can ignore them for future pulls

CREATE TABLE error_id (
    item_id TEXT,
    item_type TEXT CONSTRAINT one_of_the_three CHECK (item_type in ('book', 'author', 'user')),
    PRIMARY KEY (item_id, item_type)
);