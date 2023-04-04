#!/usr/bin/env python3
#
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


def write_records(resultPage, writer):
    # Do something with the results...
    collections = resultPage.get("dataCollections")
    # then so far collections=None
    if not collections:
        collections = resultPage.get("dataCollection")

    for collection in collections:
        title = collection["title"]
        detail = collection["self"]
        #landingPage = collection["landingPage"]["href"]
        collectionType = collection["collectionType"]
        collectionCode = collection["dataCollectionId"]
        #description = collection["description"]
    # write a row to the csv file
        row = [str(collectionCode), title, detail, collectionType]
        writer.writerow(row)
    return writer


def run_query(url, queryParams, headers):
    # Use % encoding for params
    queryParams = urllib.parse.urlencode(queryParams, quote_via=urllib.parse.quote)
    r = requests.get(url, headers=headers, params=queryParams)
    print(r.url + "\n")
    resultPage = r.json()   # A dict of the response
    return resultPage


def main():
    # define api details
    baseURL = "https://data.csiro.au/dap/ws/v2/"
    endpoint = "collections"
    url = baseURL + endpoint
    
    # open output file
    f = open('csiro_records.csv', 'w')
    # create the csv writer
    writer = csv.writer(f)
    header = ["id", "title", "url", "type"] 
    writer.writerow(header)

    # define first query
    forcode = sys.argv[1]
    #keyw = sys.argv[1]
    headers = {"Accept":"application/json"}
    queryParams = {
               #"q": "climate",
               "p": 1,
               "rpp": 100,  # maximum value
               "for": forcode,
               "soud": False,
               "showFacets": True,
               #"psd":" 2012-03-29T17:21:37+10:00",
               #"ped":" 2023-01-30T09:22:32+10:00",
               "sb": "RELEVANCE"}
    resultPage = run_query(url, queryParams, headers)
    writer = write_records(resultPage, writer)
    # Check if more than 1 page of results where returned and grab extra pages
    # Ex. last-href 'https://data.csiro.au/dap/ws/v2/collections.json?rpp=500&for=040503&soud=False&sb=RELEVANCE&p=64'
    last = resultPage['last']
    if last is not None:
        last_page = last['href'].split('&')[-1]
        npages = int(last_page[2:])
        print(f"Found {npages} pages of results")
        # if more than 1 page retrieve all
        for i in range(2, npages+1):
            queryParams['p'] = i
            resultPage = run_query(url, queryParams, headers)
            writer = write_records(resultPage, writer)

    # close the file
    f.close()

if __name__ == "__main__":
    main()
