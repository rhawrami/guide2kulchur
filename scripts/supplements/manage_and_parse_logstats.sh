#!/bin/bash

# parse logs and filter for aggregate status statements
# this is very rudimentary right now, but as I learn some more
	# bash, I'll simplify a lot of the repetitive stuff with loops

# check if logs dir exists
# define your logs dir below, or supply it with the first arg
LOGP="/PATH/TO/MY/LOGSDIR"
if [[ -n "$1" ]]; then
	LOGP="$1"
fi

if [[ ! (-e "$LOGP" && -d "$LOGP") ]]; then
	printf "logs directory not found.\n"
	exit 1
else
	printf "logs directory found: %s\n" $LOGP
fi

# there are multiple log subdirectories
# e.g., alx_ad_infinitum OR sitemap2pound
# make a directory for log statistics
if [[ ! -e "$LOGP/logstat" ]]; then
	mkdir "$LOGP/logstat"
fi

LOGSTATP="$LOGP/logstat"

for topic in $(ls $LOGP); do
	full_p="$LOGP/$topic"
	if [[ "$full_p" == "$LOGSTATP" || -f "$full_p" ]]; then
		continue
	fi
	
	# make topic subdir in logstat dir
	if [[ ! -d "${LOGSTATP}/${topic}" ]]; then
		mkdir "${LOGSTATP}/${topic}"
	fi

	# make stat files
	# summary, most important
	s_f="$LOGSTATP/${topic}/SUMMARY.txt"
	if [[ ! -f "$s_f" ]]; then
		touch $s_f
	fi

	# time elapsed
	t_e_f="$LOGSTATP/${topic}/TIME_ELAPSED.txt"
	if [[ ! -f "$t_e_f" ]]; then
		touch $t_e_f
	fi
	
	# success rate
	s_r_f="$LOGSTATP/${topic}/SUCCESS_RATE.txt"
	if [[ ! -f "$s_r_f" ]]; then
		touch $s_r_f
	fi

	# pulls per sec
	pps_f="$LOGSTATP/${topic}/PULLS_PER_SEC.txt"
	if [[ ! -f "$pps_f" ]]; then
		touch $pps_f
	fi
	
	# db insert
	db_f="$LOGSTATP/${topic}/DB_INSERT.txt"
	if [[ ! -f "$db_f" ]]; then
		touch $db_f
	fi
	
	# filter logs, update stat files
	for prog_f in $(ls "$full_p"); do
		if [[ "$prog_f" =~ ^.*prog.*$ ]]; then
			grep -i 'summary batch' "$full_p/$prog_f" >> $s_f	# update summary stat file
			grep -i 't\.e\. batch' "$full_p/$prog_f" >> $t_e_f	# update time-elapsed stat file
			grep -i 'success rate' "$full_p/$prog_f" >> $s_r_f	# update success-rate stat file
			grep -i 'pulls per sec' "$full_p/$prog_f" >> $pps_f	# update pulls-per-sec stat file
			grep -i 'db insert' "$full_p/$prog_f" >> $db_f		# update db-insert stat file
		fi
	done
	printf "\nupdated %s statistics:\n1) summary\n2) time-elapsed\n3) success-rate\n4) pulls-per-sec\n5) db-insert\n" "$topic"

	# if you run this multiple times within the same timeframe
		# you'll have duplicates
		# let's get rid of those
	sort -u "$s_f" > "${s_f}.temp" && mv "${s_f}.temp" "$s_f"
	sort -u "$t_e_f" > "${t_e_f}.temp" && mv "${t_e_f}.temp" "$t_e_f"
	sort -u "$s_r_f" > "${s_r_f}.temp" && mv "${s_r_f}.temp" "$s_r_f"
	sort -u "$pps_f" > "${pps_f}.temp" && mv "${pps_f}.temp" "$pps_f"
	sort -u "$db_f" > "${db_f}.temp" && mv "${db_f}.temp" "$db_f"
done
