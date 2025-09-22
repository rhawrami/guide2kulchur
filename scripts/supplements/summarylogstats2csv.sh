#!/bin/bash

# parse log statistics txt files, convert to csv format
# this will be used later for generating semi-real-time metrics

# first arg can be path to log statistics directory
PATH_TO_LOGSTAT="/PATH/TO/MY/LOGSTAT_DIR"
if [[ -n "$1" ]]; then 
    PATH_TO_LOGSTAT="$1"
fi

# second arg can be path to the out directory
OUT_PATH="/PATH/TO/MY/OUT_DIR"
if [[ -n "$2" ]]; then 
    OUT_PATH="$2"
fi

for i in "$PATH_TO_LOGSTAT" "$OUT_PATH"; do 
    if [[ ! -e "$i" ]]; then
        printf "bad path provided: %s\n" "$i" 
        exit 1
    fi
done

# function: summary2csv
# parse log statistics file (argument #1), convert to csv, output to outpath (argument #2)
# the sed logic is messy, but it works; may revisit in the future
summary2csv () {
    echo "date,time,batch_no,semaphore,subbatch_delay,time_elapsed,success_rate,pulls_per_sec,attempted" > "$2"
    sed -e 's/[[:alpha:]]//g' -e 's/ :: //g' -e 's/: / /g' -e 's/:=//g' -e 's/ \{2,\}/ /g' -e 's/ /,/g' "$1" >> "$2"
}

# parse log stat directory
# the dir should be composed of subject-relevant-named subdirs
# e.g., "sim_books"; "pnd2alx"
# and each of these subdirs should have a "SUMMARY.txt" file
output_delim="======================================================================================"
printf "%s\nLOGSTAT PATH: %s\nOUT_PATH: %s\n%s\n" "$output_delim" "$PATH_TO_LOGSTAT" "$OUT_PATH" "$output_delim"
for i in $(ls "$PATH_TO_LOGSTAT"); do
    if [[ -d "$PATH_TO_LOGSTAT/${i}" ]]; then
        topic_name="$i"
        for j in $(ls "$PATH_TO_LOGSTAT/${i}"); do
            if [[ "$j" == "SUMMARY.txt" ]]; then
                summary2csv "$PATH_TO_LOGSTAT/${i}/${j}" "$OUT_PATH/${topic_name}_SUMMARY.csv"
                printf "<PARSED %s >> OUT %s @ %s>\n%s\n" "$topic_name" "${topic_name}_SUMMARY.csv" "$(date)" "$output_delim"
            fi
        done
    fi
done
