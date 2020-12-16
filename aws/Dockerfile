FROM ubuntu:18.04

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    TZ=America/Chicago DEBIAN_FRONTEND=noninteractive \
    AWS_DEFAULT_REGION=us-east-1 \
    ARGS="" 

RUN apt-get update
RUN apt-get install -y awscli vim unzip wget zip git ca-certificates
RUN apt-get install -y python3 python3-pip python3-dev build-essential
RUN apt-get install -y libspatialindex-dev libgeos-dev libgdal-dev python3-gdal gdal-bin

RUN pip3 install --upgrade pip
RUN pip3 install fiona geopandas rtree psycopg2 s3fs scipy pyasn

ADD netrc /root/.netrc
RUN chmod 600 /root/.netrc

ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh

CMD fetch_and_run.sh

