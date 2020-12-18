#!/usr/bin/env python3

import os
import numpy as np
import pandas as pd
import geopandas as gpd

import gzip

from tqdm import tqdm
tqdm.pandas()

from fiona.crs import from_epsg
from shapely.geometry import Point

from pyasn import pyasn
asndb = pyasn("ipasn_20200811.dat.gz")

import maxminddb as mmdb
geolite = mmdb.open_database('geolite2.mmdb')

import IP2Location as ip2loc
ip2loc_v4 = ip2loc.IP2Location("ip2loc_ipv4_db5.bin")
ip2loc_v6 = ip2loc.IP2Location("ip2loc_ipv6_db5.bin")

subnets = pd.read_csv("s3://jsaxon-cdac-unacast/ip/inputs/subnet_cidr.csv.gz")
cidr    = pd.read_csv("s3://jsaxon-cdac-unacast/ip/inputs/cidr.csv.gz", usecols = ["CIDR", "DBA"])
subnets = subnets.merge(cidr)


import vincenty as vcty

def vincenty(row, A = "", B = ""):
    
    if A: A += "_"
    if B: B += "_"
    
    a = (row["{}lat".format(A)], row["{}lon".format(A)])
    b = (row["{}lat".format(B)], row["{}lon".format(B)])
    
    return vcty.vincenty(a, b)



def get_locations(x):
    
    loc = {"ip_address" : x, 
           "mm_lat" : np.nan, "mm_lon" : np.nan, "mm_acc" : np.nan, 
           "ip2loc_lat" : np.nan, "ip2loc_lon" : np.nan 
           # "mm_loc" : np.nan, ip2loc_loc" : np.nan
           }
    
    mm_ret = geolite.get(x)
    if mm_ret is not None and "location" in mm_ret:
        mm_loc = mm_ret["location"]
        loc["mm_lat"] = mm_loc["latitude"]
        loc["mm_lon"] = mm_loc["longitude"]
        loc["mm_acc"] = mm_loc["accuracy_radius"]
        # loc["mm_loc"] = Point([loc["mm_lon"], loc["mm_lat"]])


    if "." in x: ip2loc_ret = ip2loc_v4.get_all(x)
    else:        ip2loc_ret = ip2loc_v6.get_all(x)
        
    if ip2loc_ret.country_short != "-":
        loc["ip2loc_lat"] = ip2loc_ret.latitude
        loc["ip2loc_lon"] = ip2loc_ret.longitude
        # loc["ip2loc_loc"] = Point([loc["ip2loc_lon"], loc["ip2loc_lat"]])
        
    return pd.Series(loc)


from ipaddress import ip_address, ip_network
def subnet(x):

    x = str(x)

    if "." in x:   return str(ip_network(x + "/24", strict = False)).replace(".0/24", ".0")
    elif ":" in x: return str(ip_network(x + "/48", strict = False)).replace("/48", "").lower()
    return ""


tracts = gpd.read_file("s3://jsaxon-cdac-unacast/ip/inputs/city_tracts.geojson")


import gzip, boto3
from io import BytesIO, TextIOWrapper

s3_output_bucket = "jsaxon-cdac-unacast"
s3_output_key    = "proc/data/{}.csv.gz"
def write_data_to_s3(df, label):

    zip_buff = BytesIO()
    with gzip.GzipFile(mode='w', fileobj = zip_buff) as zip_file:
        df.to_csv(TextIOWrapper(zip_file, 'utf8'),
                  index = False)
    
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(s3_output_bucket,
                                   s3_output_key.format(label))
    s3_object.put(Body = zip_buff.getvalue())



def supplement_fields(input_file, output_label):

    df = pd.read_csv(input_file, 
                     usecols = ["identifier", "ip_address",
                                "latitude", "longitude", "local_date_time",
                                "duration", "bump_count", "classification"])

    df.query("~ip_address.isnull()", engine = 'python', inplace = True)
    df.reset_index(drop = True)
    print(df.shape[0], df.memory_usage().sum() / 1e9)

    #### NIGHT: if ...
    df["ts"] = pd.to_datetime(df["local_date_time"])
    df["S"] = df.ts.dt.hour * 3600 + df.ts.dt.minute * 60 + df.ts.dt.second
    df["night"] = 0

    # - it begins and ends on different days -- and ends after 2am.
    df.loc[df.S + df.duration > 26 * 3600, "night"] = 1
    
    # - start is before 2am and end is after 6am (lasts > 4 hours).
    df.loc[(df.S < 2 * 3600) & (df.duration > 4 * 3600), "night"] = 1

    #### GEOID
    df = gpd.GeoDataFrame(data = df, crs = "EPSG:4326",
                          geometry = gpd.points_from_xy(x = df.longitude,
                                                        y = df.latitude))

    print(df)
    df["geoid"] = gpd.sjoin(df, tracts, op = "within").geoid

    #### X, Y
    df.to_crs(epsg = 2163, inplace = True)
    df["x"] = df.geometry.x.round(1)
    df["y"] = df.geometry.y.round(1)

    df.drop("geometry", inplace = True, axis = 1)

    df.reset_index(drop = True, inplace = True)

    df.rename(columns = {"latitude" : "lat", "longitude" : "lon"}, inplace = True)
    df["lat"] = df.lat.round(5)
    df["lon"] = df.lon.round(5)

    df["ip_address"] = df["ip_address"].str.lower()
    df["subnet"] = df.ip_address.apply(subnet)
    df = df.merge(subnets)

    unique_ip = df.ip_address.sort_values().drop_duplicates().reset_index(drop = True)
    
    tqdm.pandas(desc = "Get IP Locations    ::::::: ")
    ip_locations = unique_ip.progress_apply(get_locations)
    
    tqdm.pandas(desc = "MaxMind to IP2Loc   ::::::: ")
    ip_locations["mm_ip_dist"] = ip_locations[["ip2loc_lat", "ip2loc_lon", "mm_lat", "mm_lon"]]\
                                             .progress_apply(vincenty, A = "mm", B = "ip2loc", axis = 1)
    
    df = df.merge(ip_locations)
    
    tqdm.pandas(desc = "GPS to IP2Loc       ::::::: ")
    df["gps_ip_dist"] = df[["lat", "lon", "ip2loc_lat", "ip2loc_lon"]]\
                          .progress_apply(vincenty, A = "ip2loc", axis = 1)
    
    tqdm.pandas(desc = "GPS to MaxMind      ::::::: ")
    df["gps_mm_dist"] = df[["lat", "lon", "mm_lat", "mm_lon"]]\
                          .progress_apply(vincenty, A = "mm", axis = 1)
    
    df["ASN"] = df.ip_address.apply(lambda x: asndb.lookup(x)[0])
    df["ASN"] = df["ASN"].replace({np.nan : -1}).astype(int)

    df.drop(["ts", "S"], axis = 1, inplace = True)
    
    write_data_to_s3(df, output_label)


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description='Append extra fields to GPS data.')
    parser.add_argument("-s", "--skip", type = int, default = 0)
    parser.add_argument("-n", "--nfiles", type = int, default = 10)
    args = parser.parse_args()

    s3_input_sample = "s3://jsaxon-cdac-unacast/sample/samples/"
    s3_input_data   = "cluster_chicago_philly_nyc_40km/data{:012d}.csv.gz"

    s3_input_template = s3_input_sample + s3_input_data


    for v in range(args.skip, args.skip + args.nfiles):

        input_file  = s3_input_template.format(v)
        output_file = test_file.split("/")[-1].replace(".csv.gz", "")

        supplement_fields(test_file, output_file)


