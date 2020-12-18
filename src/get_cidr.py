#!/usr/bin/env python3

import sys

import pandas as pd

from tqdm import tqdm
tqdm.pandas()

from cidr import *
cidr_search = CIDR("../data/cidr.csv")


TOTAL = 5000
N_PER = 250
s3_subnets = "s3://jsaxon-cdac-unacast/proc/subnets/sn{:05d}.csv.gz"

if False:

    subnets = set()
    for x in range(0, TOTAL, N_PER):

        subnet_df = pd.read_csv(s3_subnets.format(x // N_PER))
        subnets |= set(subnet_df.subnet)

    subnets_df = pd.DataFrame({"subnet" : list(subnets)})
    subnets_df.sort_values(by = "subnet", inplace = True)
    subnets_df.to_csv("../data/local_subnets.csv", index = False, header = False)


subnets_df = pd.read_csv("../data/local_subnets.csv", names = ["subnet"])

subnets_df["CIDR"] = subnets_df.subnet.progress_apply(cidr_search.find_cidr)
subnets_df.to_csv("../data/subnet_cidr.csv.gz", index = False, header = True)


