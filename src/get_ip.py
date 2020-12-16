#!/usr/bin/env python 

import os, sys

import pandas as pd

from ipaddress import ip_address, ip_network

from tqdm import tqdm

import gzip, boto3
from io import BytesIO, TextIOWrapper

from netrc import netrc
user, acct, passwd = netrc().authenticators("aws-unacast")

os.environ["AWS_ACCESS_KEY_ID"]     = user
os.environ["AWS_SECRET_ACCESS_KEY"] = passwd

import argparse
parser = argparse.ArgumentParser(description='Label points in a video.')
parser.add_argument("-s", "--skip", type = int, default = 0)
parser.add_argument("-n", "--nfiles", type = int, default = 10)
args = parser.parse_args()


def subnet(x):

    x = str(x)

    if "." in x:   return str(ip_network(x + "/24", strict = False)).replace(".0/24", ".0")
    elif ":" in x: return str(ip_network(x + "/48", strict = False)).replace("/48", "").lower()
    return None



s3_input_sample = "s3://jsaxon-cdac-unacast/sample/samples/"
s3_input_data   = "cluster_chicago_philly_nyc_40km/data{:012d}.csv.gz"
s3_input_file   = s3_input_sample + s3_input_data

def get_unique_subnets(offset, nfiles):

    unique_net = set()
    for x in tqdm(range(offset, offset + nfiles)):
    
        df = pd.read_csv(s3_input_file.format(x))
        unique_net |= set(df.ip_address.dropna().apply(subnet))
    
    unique_net = pd.DataFrame({"subnet" : list(unique_net)})

    return unique_net



s3_output_bucket = "jsaxon-cdac-unacast"
s3_output_key    = "proc/subnets/{}.csv.gz"

def write_subnets_df_to_s3(df, label):

    zip_buff = BytesIO()
    with gzip.GzipFile(mode='w', fileobj = zip_buff) as zip_file:
        df.to_csv(TextIOWrapper(zip_file, 'utf8'),
                  index = False)
    
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(s3_output_bucket,
                                   s3_output_key.format(label))
    s3_object.put(Body = zip_buff.getvalue())


# Do stuff....
SKIP, NFILES = args.skip, args.nfiles
unique_nets = get_unique_subnets(offset = SKIP, nfiles = NFILES)
write_subnets_df_to_s3(unique_nets, "sn{:05d}".format(SKIP // NFILES))


