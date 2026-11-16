# Historical scripts

These are the original research scripts, moved without changes. Directory names,
numeric prefixes, and ordering are preserved. They are reference material, **not
maintained entry points**. Many contain fixed paths, run sizes, sleeps, broad
exception handling, or destructive staging-table operations. Do not run them as
part of the new pipeline.

## Original collection sequence

| Steps | Purpose |
| --- | --- |
| main-pipeline/00–01 | Discover genres and seed book IDs |
| main-pipeline/02–03 | Collect top-shelved / recently read books |
| main-pipeline/04, 07 | Fill book recommendation links (07 superseded 04) |
| main-pipeline/05 | Expand books through recommendations |
| main-pipeline/06 | Collect authors referenced by books |
| main-pipeline/08–09 | Fill author recommendations; expand authors |
| main-pipeline/10 | Collect authors from sitemap IDs |
| main-pipeline/11 | Collect books referenced by authors |
| main-pipeline/12–13 | Seed users from sitemaps; expand social links |
| main-pipeline/14–15 | Discover books/authors from user profiles |

`goodreads-choice-awards/` collected awards snapshots. `supplements/` contains
geocoding, Wikidata, sitemap, awards-loading, and historical log tools.
`ex-post/` contains heuristic data updates. `visuals/` contains the original map,
network, and demographic analyses. Historical figures may depend on manually
selected thresholds or exports and are not promised to reproduce byte-for-byte.

The package's `privateer/` and `engineer/` modules retain their original names
and import paths for compatibility. No archived script is imported by maintained
commands. Git history remains the source for their development history.
