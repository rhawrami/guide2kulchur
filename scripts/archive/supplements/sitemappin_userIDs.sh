#!/bin/bash

# pull Goodreads user IDs from public sitemap
# first, pull the user index, then for each xml in the index, pull only the IDs
# returns compressed and uncompressed text file with 
# this script should be run from the base 'guide2kulchur' dir 

OUT_DELIM="================================================================================="
printf "%s\n" "$OUT_DELIM"

# again, run from "./guide2kulchur"
if [[ "$(basename $(pwd))" == "guide2kulchur" ]]; then
    SITEMAP_DIR="data/sitemap-dat"
    OUTFILE_PATH="data/sitemap-dat/final_userIDs_from_sitemap.txt"
else
    printf "please run this script from base 'guide2kulchur' dir, not %s\n" "$(basename $(pwd))"
    exit 1
fi

> "$OUTFILE_PATH"

# Pull site-indices for User IDs
MAIN_SITEMAP_LOC="https://www.goodreads.com/siteindex.user.xml"
USR_SITEMAP_LOCS=$(curl -s "$MAIN_SITEMAP_LOC" | grep -iE 'xml.gz' | sed -r -e 's,</?loc>,,g' -e 's/ +//g')

# For each index, extract the ID from each Goodreads User URL
for i in $USR_SITEMAP_LOCS; do
    curl -s "$i" | gzcat | grep '<loc>.*</loc>' | grep -Eo '/\d+-' | sed -r -e 's,/,,g' -e 's,-,,g' >> "$OUTFILE_PATH"
    printf "processed %s @ %s\n" "$i" "$(date +'%H:%M:%S %m/%d/%y')"
done

# In case there are any duplicates
mv "$OUTFILE_PATH" "${OUTFILE_PATH}.old"
sort -n "${OUTFILE_PATH}.old" | uniq > "$OUTFILE_PATH"
rm "${OUTFILE_PATH}.old"
# if you run this twice, just rewrite when prompted
gzip -k "$OUTFILE_PATH"

COUNT_IDS_PULLED="$(wc -l $OUTFILE_PATH)"
printf "%s\ncompleted User sitemap extraction @ %s\n# User IDs: %s\n%s\n" "$OUT_DELIM" "$(date +'%H:%M:%S %m/%d/%y')" "$COUNT_IDS_PULLED" "$OUT_DELIM"