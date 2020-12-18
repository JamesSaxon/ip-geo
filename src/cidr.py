#!/usr/bin/env python 

import pandas as pd
import numpy as np
from ipaddress import ip_address, ip_network

import sys
import re
import requests


dba_refs = {
  "AT&T" : "ATT", "Verizon" : "Verizon", "Comcast" : "Comcast",
  "University of Chicago" : "UChicago",
  "Wide Open West" : "WOW", "WIDEOPENWEST" : "WOW", "WideOpenWest" : "WOW", "WIDE OPEN WEST" : "WOW",
  "T-Mobile" : "T-Mobile", "Sprint" : "Sprint",
  "Illinois Century Network" : "ICN",
  "ALPINE" : "Alpine", "Cox" : "Cox", "Google" : "Google",
  "Illinois Institute of Technology" : "IIT",
  "Internet Assigned Numbers Authority" : "IANA",
  "Windstream Communications": "Windstream",
  "City of Chicago" : "City of Chicago",
  "Everywhere Wireless" : "Everywhere",
  "JOHN NAPOLITANO" : "John Napolitano",
  "Qwest Communications" : "Qwest",
  "Remax" : "Remax", "Hughes Network Systems" : "Hughes",
  "OSI Group" : "OSI", "SilverIP" : "SilverIP",
  "Synergy Internet" : "Synergy Internet",
  "Webpass" : "Webpass", "Network Device" : "Access One", ## See parent
  "OnShore" : "OnShore", "RCN" : "RCN",
  "Wayport" : "Wayport", "rback8c.akrnoh" : "ATT", ## See parent
  'Charter Communications' : "Charter",
  'Asia Pacific Network Information Centre' : "APNIC",
  'RIPE Network Coordination Centre' : "RIPE",
  'CenturyLink' : "CenturyLink",
  'Cablevision' : "Cablevision",
  'Frontier Communications' : "Frontier",
  'DigitalOcean' : "DigitalOcean",
  'African Network Information Center' : "AFRINIC",
  'VPN' : "VPN",
  'Level 3' : "Level 3",
  'IBM' : "IBM",
  'CABLE ONE' : "Cable One",
  'Mediacom Communications Corporation' : "Mediacom",
  'DoD Network Information Center' : "DoD",
  'Amazon Technologies Inc.' : "Amazon",
  'ViaSat,Inc.' : "ViaSat",
  'Consolidated Communications' : "Consolidated Communications",
  'MegaPath Corporation' : "MegaPath",
  'Fuse Internet Access' : "Fuse",
  'MTCO Communications' : "MTCO Communications",
  'Hosting Services, Inc.' : "Hosting Services",
  'JAB Wireless, INC.' : "JAB Wireless",
  'TDS TELECOM' : "TDS Telecom",
  'Armstrong' : "Armstrong",
  'Atlantic Broadband Finance, LLC' : "Atlantic Broadband Finance",
  'Strong Technology LLC' : "Strong Technology",
  'Alaska Communications Systems Group, Inc.' : "Alaska Communications",
  'Total server solutions' : 'Total server solutions',
  'Microsoft Corporation': "Microsoft",
  'NTT America, Inc.' : "NTT America",
  'Suddenlink Communications' : "Suddenlink Communications",
  'CloudRoute, LLC' : "Cloudroute",
  'OVH Hosting, Inc.' : "OVH Hosting",
  'Leaseweb USA, Inc.' : "Leasweb USA",
  'Bigleaf Networks, Inc.' : "Bigleaf Networks",
  'SONIC' : "Sonic",
  'MegaPath Networks Inc.' : "MegaPath",
  'Rogers Communications Canada Inc.' : "Rogers Communications",
  'TierPoint, LLC' : "TierPoint",
  'Atlantic Metro Communications II, Inc.' : "Atlantic Metro Communications",
  'CBEYOND COMMUNICATIONS, LLC' : "CBeyond Communications",
  'Zayo Bandwidth' : "Zayo",
  'DHCP Pool - Cinergy MetroNet' : "Cinergy",
  'GTT' : "GTT",
  'KNOLOGY,  Inc.' : "Knology",
  'The Procter and Gamble Company' : "Proctor and Gamble",
  'Adobe Systems Incorporated' : "Adobe",
  '1&1 IONOS Inc.' : "1&1",
  'Massillon Cable Com' : "Massillon Cable",
  'Cogent Communications' : "Cogent",
  'Hewlett-Packard Company' : "HP",
  'CERFnet' : "CERFnet", 'SingleHop' : "SingleHop",
  "Zscaler" : "Zscaler", "JPMorgan" : "JPMorgan",
  "tw telecom" : "TW Telecom",
  "Latin American and Caribbean IP address Regional Registry" : "LACNIC",
  "Vultr Holdings, LLC" : "Vultr Holdings",
  "Sympatico HSE" : "Sympatico HSE",
  "Private Customer" : "Private",
  "Midcontinent Communications" : "Midcontinent Communications",
  "Linode" : "Linode", 
  "Nobis Technology" : "Nobis", 
  "Total Server Solutions L.L.C." : "Total Server Solutions",
  "QuadraNet" : "QuadraNet",
  "B2 Net Solutions Inc." : "B2 Net",
  "SecuredConnectivity.net" : "SecuredConnectivity",
  "New Wave Communications" : "New Wave Communications",
  "Videotron" : "Videotron",
  "Shaw Communications Inc." : "Shaw Communications",
  "Network Equipment" : "Network Equipment",
  "Global Capacity" : "Global Capacity",
  "Amanah Tech Inc." : "Amanah Tech",
  "Nexeon Technologies, Inc." : "Nexeon",
  "PenTeleData House Account" : "PenTeleData",
  "PSINet" : "PSINet",
  'Agilent Technologies': 'Agilent',
  'Amazon Data Services': 'Amazon Data Services',
  'Amazon.com': 'Amazon.com',
  'American Airlines': 'American Airlines',
  'Apple': 'Apple',
  'Argonne National Laboratory': 'ANL',
  'Bell Canada': 'Bell Canada',
  'Cloudflare, Inc.': 'Cloudflare',
  'Fermi National Accelerator Laboratory (Fermilab)': 'FNAL',
  'Massachusetts Institute of Technology': 'MIT',
  'McKinsey & Company, Inc.': 'McKinsey & Company',
  'Northern Illinois University': 'NIU',
  'UBS AG': 'UBS',
  'United Airlines': 'United Airlines',
  'United States Postal Service': 'USPS',
  'Viacom': 'Viacom',
  'Vodafone': 'Vodafone',
  'Wal-Mart Stores, Inc.': 'Walmart',
  "Bank of America" : "Bank of America"
}


def subnet(x):

    x = str(x)

    if "." in x:   return str(ip_network(x + "/24", strict = False)).replace(".0/24", ".0")
    elif ":" in x: return str(ip_network(x + "/48", strict = False)).replace("/48", "").lower()
    return ""


def addr_in_subnet(addr, net): return ip_address(addr) in ip_network(net)


def get_isp(x):
    
    isp = cidr_df[cidr_df.net.apply(lambda y: ip_address(x) in y)].DBA
    
    if isp.shape[0]: return isp.iloc[0]




def query_whois(ip = None, url = None):

    if url is None:
        url = "http://whois.arin.net/rest/ip/{}".format(ip)

    blocks = []

    try:

        j = requests.get(url, headers = {'Accept': 'application/json'}).json()
        
        if "orgRef" in j["net"]:
            org = j["net"]["orgRef"]["@name"]
        elif "customerRef" in j["net"]:
            org = j["net"]["customerRef"]["@name"]
            
        net_block = j["net"]["netBlocks"]["netBlock"]

        if type(net_block) is dict:
            net_block = [net_block]

        for nb in net_block:

            ipv = 4 if "." in ip else 6
            bits = int(nb["cidrLength"]["$"])

            if (ipv == 4 and bits > 24) or (ipv == 6 and bits > 48):

                parent_url = j["net"]["parentNetRef"]["$"]
                return query_whois(url = parent_url, ip = ip)

            new_cidr = nb["startAddress"]["$"] + "/" + nb["cidrLength"]["$"]

            new_dba = ""
            for ref, dba in dba_refs.items():
                if ref in org: new_dba = dba

            net = ip_network(new_cidr)

            blocks.append({"DBA" : new_dba, "Organization" : org, "CIDR" : new_cidr, 
                           "net" : net, "bits" : bits, "IPV" : ipv})
            
    except: 

        print("failed on", url)

    return blocks




class CIDR():

    def __init__(self, cache_file = "cidr.csv"):

        self.cache_file = cache_file

        self.cache = pd.read_csv(cache_file)
        self.cache["net"] = self.cache.CIDR.apply(ip_network)
    

    def update_cache(self, ip): 

        netblocks = query_whois(ip)

        netblocks_df = pd.DataFrame(netblocks)

        self.cache = self.cache.append(netblocks_df).reset_index(drop = True)


    def save_cache(self):
    
        for org in self.cache.Organization.unique():
            for ref, dba in dba_refs.items():
                if ref in org:
                    self.cache.loc[self.cache.Organization == org, "DBA"] = dba
    
        self.cache.sort_values(by = ["DBA", "CIDR"], inplace = True)
        self.cache[["DBA", "Organization", "CIDR", "bits"]].to_csv(self.cache_file, index = False)

    def get_cached(self, ip):

        addr = ip_address(ip)

        cidr = self.cache[self.cache.net.apply(lambda net: addr in net)].CIDR
        if cidr.shape[0]: return cidr.iloc[0]

        return None


    def find_cidr(self, ip):
    
        if ip is np.nan: return ""

        ## First check if we've already cached the result.
        cached_result = self.get_cached(ip)
        if cached_result is not None: return cached_result

        self.update_cache(ip)
        self.save_cache()
    
        ## Try again: the simple look-up should now work.
        cached_result = self.get_cached(ip)
        if cached_result is not None: return cached_result
    
        return ""

   
 
