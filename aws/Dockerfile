FROM ubuntu:18.04

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    TZ=America/Chicago DEBIAN_FRONTEND=noninteractive \
    AWS_DEFAULT_REGION=us-east-1 \
    ARGS="" 

RUN apt-get update
RUN apt-get install -y vim unzip wget zip git ca-certificates
RUN apt-get install -y python3 python3-pip python3-dev build-essential
RUN apt-get install -y libspatialindex-dev libgeos-dev libgdal-dev python3-gdal gdal-bin

RUN pip3 install --upgrade pip
RUN pip3 install fiona geopandas rtree psycopg2 s3fs scipy pyasn tqdm vincenty maxminddb IP2Location 

## Somebody at Amazon fucked up.
RUN pip3 install botocore==1.18.14 awscli==1.18.155 boto3==1.15.14

ENV AWS_ACCESS_KEY_ID=<my_key> AWS_SECRET_ACCESS_KEY=<my_secret_key>

ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh
CMD fetch_and_run.sh

