#!/bin/bash

# Be sure to use the virtualenvironment
cd /home/jc2a23/parsing_rse_job_adverts/
source .venv/bin/activate

echo "WEKLY TASK BEGINNING"

rm log_weekly_stdout.txt
rm log_weekly_stderr.txt
# Download new jobs:
python  pipeline.py --no-api --no-tar > log_weekly_stdout.txt 2> log_weekly_stderr.txt

# Save most recent run to log file. Outputs of this script will also be logged to /var/log/syslog by the crontask
LOG_STDOUT=$( cat log_weekly_stdout.txt )
echo "DAY TASK STDOUT: $LOG_STDOUT"
LOG_STDERR=$( cat log_weekly_stderr.txt )
echo "DAY TASK STDERR: $LOG_STDERR"

# Send email report
if [ -s "log_weekly_stderr.txt" ]; then
    # Stderr log file is not empty
    EMAIL_SUBJECT="Jobs analysis weekly report -- ERRORS FOUND"
elif [[ $LOG_STDOUT = *'WARNING'* ]]; then
    EMAIL_SUBJECT="Jobs analysis weekly report -- WARNINGS RAISED"
else
    # Stderr log file is empty
    EMAIL_SUBJECT="Jobs analysis weekly report"
    LOG_STDERR="None"
fi
EMAIL_BODY="Jobs analysis daily job scraper (running on srv04851) report: \n\nOutput: \n${LOG_STDOUT} \n\nErrors: \n${LOG_STDERR}"

echo -e "${EMAIL_BODY}" | s-nail -s "${EMAIL_SUBJECT}" -A gmail "j.s.robinson@soton.ac.uk"
echo -e "${EMAIL_BODY}" | s-nail -s "${EMAIL_SUBJECT}" -A gmail "sjh@ecs.soton.ac.uk"
echo -e "${EMAIL_BODY}" | s-nail -s "${EMAIL_SUBJECT}" -A gmail "jc2a23@soton.ac.uk"

echo "WEEKLY TASK END"
