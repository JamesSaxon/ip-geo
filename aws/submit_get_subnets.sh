#!/bin/bash 

script=get_subnets.py
src="../src"

s3_base=s3://jsaxon-cdac-unacast/ip/scripts
s3_script=${s3_base}/${script}

N_PER_JOB=250
MAX_FILES=4999

version=01c

aws s3 cp ${src}/${script} ${s3_base}/

for x in $(seq -w 0 ${N_PER_JOB} ${MAX_FILES}); do

    xtrunc=$(echo $x | sed "s/^0*//")
    if [ "$xtrunc" == "" ]; then xtrunc=0; fi

    aws batch submit-job \
      --job-name uc-${x}-${version} --job-queue geo-queue \
      --job-definition arn:aws:batch:us-east-1:931898435070:job-definition/geo-def-small:3 \
      --container-overrides '{"environment" : [{"name" : "SCRIPT", "value" : "'${s3_script}'"}, {"name" : "ARGS", "value" : "-s '${xtrunc}' -n '${N_PER_JOB}'"}]}'

done

