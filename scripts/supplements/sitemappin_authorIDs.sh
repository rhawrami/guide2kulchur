#!/bin/bash

# pull extant author IDs from Goodreads, using their sitemap
# Sitemap links located at their robots txt: https://www.goodreads.com/robots.txt

# mkdir for everything
mkdir data/sitemap-dat/temp_unzipped_sitemaps
cd data/sitemap-dat/temp_unzipped_sitemaps

# Author sitemap here: https://www.goodreads.com/siteindex.author.xml
# curl it, get all sitemap links
curl --silent https://www.goodreads.com/siteindex.author.xml | grep -Eio 'https.*gz' > temp_sitemaps.txt

# make main file for everything
> final_authorIDs_from_sitemap.txt

# download each sitemap, decompress it
for i in $(cat temp_sitemaps.txt | grep -Eo '\d+'); do
	  curl --silent -o sm_$i.xml.gz https://www.goodreads.com/sitemap.{$i}.xml.gz && echo "download sm_$i @ $(date)"
	  gunzip sm_$i.xml
	  grep -Eio 'author/show/.*$' sm_$i.xml | grep -Eo '\d+' >> author_xmls.txt;
	done;

# clean up any duplicates
sort author_xmls.txt | sed -E 's/^0+//' | sort -n | uniq > ../final_authorIDs_from_sitemap.txt
# gzip it
gzip ../final_authorIDs_from_sitemap.txt

cd ..
echo '-------------------------------------------------------'
echo "final author IDs are located in 'final_authorIDs_from_sitemap.txt.gz' .
run 'rm -r data/sitemap-dat/temp_unzipped_sitemaps' to get rid of unneeded files."
