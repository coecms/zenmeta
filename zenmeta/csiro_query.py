#!/usr/bin/env python3

'''
Search for Data Access Portal collections

Requires the Requests module: http://docs.python-requests.org/en/master/
Try:
  pip install requests
'''

import json
import urllib
import requests
import sys
import csv

# open output file
f = open('csiro_records.csv', 'w')
# create the csv writer
writer = csv.writer(f)
header = ["title", "url", "type", "id", "description"] 
writer.writerow(header)


# ws.data.csiro.au/collections and data.csiro.au/dap/ws/v2 behave similarly,
# but there are a few differences in the response.
# Here we use /ws/v2 and comment out ws.data.csiro.au (deprecated).
#baseURL = "https://ws.data.csiro.au/"
baseURL = "https://data.csiro.au/dap/ws/v2/"
endpoint = "collections"
url = baseURL + endpoint

headers = {"Accept":"application/json"}

# All query parameters are optional.
# If none are used, all accessible metadata will be returned.
#
#    q:      query
#    p:      page
#    rpp:    results per page
#    soud:   show only unrestricted data
#            (i.e. only results you can download files from)
#    psd:    publication start date, use ISO 8601 format,
#            e.g. "2017-03-29T17:21:37+10:00"
#            Works in /ws/v2 only.
#            Note that the Requests module will URL-encode these values
#            for you.  If constructing the URL another way the value would
#            need to be:
#            e.g. "2017-03-29T17%3A21%3A37%2B10%3A00"
#    ped:    publication end date, use ISO 8601 format,
#            e.g. "2018-03-29T17:21:37+10:00"
#            Works in /ws/v2 only.
#            Note that the Requests module will URL-encode these values
#            for you.  If constructing the URL another way the value would
#            need to be:
#            e.g. "2018-03-29T17%3A21%3A37%2B10%3A00"
#    sb:     sort by: "RELEVANCE", "RECENT" or "TITLE"
queryParams = {"q":"climate",
               "p":1,
               "for": "040104",
               "soud":False,
               #"psd":"2012-03-29T17:21:37+10:00",
               #"ped":"2017-12-29T09:22:32+11:00",
               "sb":"RELEVANCE"}
# Use % encoding for params
queryParams = urllib.parse.urlencode(queryParams, quote_via=urllib.parse.quote)

r = requests.get(url, headers=headers, params=queryParams)
#print(r.url + "\n")

resultPage = r.json()   # A dict of the response

# Do something with the results...
collections = resultPage.get("dataCollections")
# ws.data.csiro.au (deprecated) uses "dataCollection", if you're using that
# then so far collections=None
if not collections:
    collections = resultPage.get("dataCollection")

for collection in collections:
    title = collection["title"]
    detail = collection["self"]
    #landingPage = collection["landingPage"]["href"]
    collectionType = collection["collectionType"]
    collectionCode = collection["dataCollectionId"]
    description = collection["description"]
    # write a row to the csv file
    row = [title, detail, collectionType, str(collectionCode), description]
    writer.writerow(row)

# close the file
f.close()
