-- RAN:
    -- Wed Sep 9 2025

-- add index on pound.sim_authors
-- reason for change:
    -- see migration file #003, same logic but for authors table now

CREATE INDEX idx_sim_authors on pound USING gin(sim_authors);