#!/bin/bash 

script=append.py
src="../src"

s3_base=s3://jsaxon-cdac-unacast/ip/scripts
s3_script=${s3_base}/${script}

N_PER_JOB=2
MAX_FILES=1

version=01a

aws s3 cp ${src}/${script} ${s3_base}/

extra_files=""
extra_base="s3://jsaxon-cdac-unacast/ip/inputs"
for d in geolite2.mmdb ip2loc_ipv4_db5.bin ip2loc_ipv6_db5.bin ipasn_20200811.dat.gz; do 
  extra_files=${extra_base}/${d}" "${extra_files}
done

for x in $(seq -w 0 ${N_PER_JOB} ${MAX_FILES}); do

    xtrunc=$(echo $x | sed "s/^0*//")
    if [ "$xtrunc" == "" ]; then xtrunc=0; fi

    aws batch submit-job \
      --job-name uc-${x}-${version} --job-queue geo-queue \
      --job-definition arn:aws:batch:us-east-1:931898435070:job-definition/geo-def-small:3 \
      --container-overrides "{\"environment\" : [{\"name\" : \"SCRIPT\", \"value\" : \"${s3_script}\"}, {\"name\" : \"ARGS\", \"value\" : \"-s ${xtrunc} -n ${N_PER_JOB}\"}, {\"name\" : \"EXTRA_FILES\", \"value\" : \"${extra_files}\"}]}"

done

