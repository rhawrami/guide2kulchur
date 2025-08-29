-- RAN:
    -- Mon Aug 29 2025

-- add index on alexandria.sim_books
-- reason for change:
    -- using the sim_books column in alexandria, I can keep pulling new books based on their IDs
    -- but this process will continue taking longer as I add more and more rows
    -- there are a number of empty

CREATE INDEX idx_sim_books on alexandria USING gin(sim_books);