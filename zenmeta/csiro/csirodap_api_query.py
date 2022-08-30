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
               "rpp": 25,
               "for": "040503",
               "soud":False,
               #"psd":"2012-03-29T17:21:37+10:00",
               #"ped":"2017-12-29T09:22:32+11:00",
               "sb":"RELEVANCE"}
# Use % encoding for params
queryParams = urllib.parse.urlencode(queryParams, quote_via=urllib.parse.quote)

r = requests.get(url, headers=headers, params=queryParams)
print(r.url + "\n")

resultPage = r.json()   # A dict of the response
#print(resultPage)

# Do something with the results...
collections = resultPage.get("dataCollections")
# ws.data.csiro.au (deprecated) uses "dataCollection", if you're using that
# then so far collections=None
if not collections:
    collections = resultPage.get("dataCollection")

allkeys=set()
for collection in collections:
    keys = [x for x in collection.keys()]
    for y in keys:
        allkeys.add(y)
print(allkeys)
sys.exit()
for collection in collections:
    title = collection["title"]
    detail = collection["self"]
    landingPage = collection["landingPage"]["href"]
    collectionType = collection["collectionType"]
    collectionCode = collection["dataCollectionId"]
    description = collection["description"]
    #forCodes = collection["fieldsOfResearch"]
    #date_from = collection["dataStartDate"]
    #date_to = collection["dataEndDate"]
    pub_date = collection["published"]
    #keys = collection["keywords"]
    license = collection["licence"]
    attribution = collection["attributionStatement"]
    doi = collection["doi"]
    contributors = collection["contributors"]
    spatial = collection["spatialParameters"]
    lead = collection["leadResearcher"]
    # = collection[""][""]
    print("Title: {0}\n"
          "Detail: {1}\n"
          "Landing Page: {2}\n".format(title, detail, landingPage))
    print(collectionType)
    print(collectionCode)
    print(description)
    print(forCodes)
    print(keys)
    print(date_from)
    print(date_to)
    print(doi)
    print(attribution)
    print(contributors)
