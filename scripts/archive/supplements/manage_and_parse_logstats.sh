#!/bin/bash

# parse logs and filter for aggregate status statements
# this is very rudimentary right now, but it works for the time being

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

	printf "==============================================================================\n"
	# for each statistic (e.g., TIME_ELAPSED)
		# 1. make a txt file to store the related logs
		# 2. check all the *_prog.log.* logs and filter for the related stat statements
		# 3. pass the related lines to the txt file
		# 4. remove duplicates from the txt file
	for subj in "SUMMARY" "TIME_ELAPSED" "SUCCESS_RATE" "PULLS_PER_SEC" "DB_INSERT"; do
		subj_f="$LOGSTATP/${topic}/${subj}.txt"
		if [[ ! -f "$subj_f" ]]; then
			touch $subj_f
		fi
		
		for prog_f in $(ls "$full_p"); do
			if [[ "$prog_f" =~ ^.*prog.*$ ]]; then
				case "$subj" in
					"SUMMARY")
						key_term="summary batch"
						;;
					"TIME_ELAPSED")
						key_term="t\.e\. batch"
						;;
					"SUCCESS_RATE")
						key_term="success rate"
						;;
					"PULLS_PER_SEC")
						key_term="pulls per sec"
						;;
					"DB_INSERT")
						key_term="db insert"
						;;
				esac
				grep -i "$key_term" "${full_p}/${prog_f}" >> $subj_f	# filter lines for each subject statistic
			fi
		done

		sort -u "$subj_f" > "${subj_f}.temp" && mv "${subj_f}.temp" "$subj_f"	# filter out duplicates
		printf "processed <%s> :: %s @ %s\n" "$topic" "$subj" "$(date)"

	done
	printf "==============================================================================\n"
done
