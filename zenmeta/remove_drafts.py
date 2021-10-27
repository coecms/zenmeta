#!/usr/bin/env python
# coding: utf-8
# Copyright 2021 ARC Centre of Excellence for Climate Extremes
# author: Paola Petrelli <paola.petrelli@utas.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
import json
from datetime import date
from os.path import expanduser
import sys
from .utils import get_token, read_json, get_bucket

def get_drafts(url, token, community, community_id):
    """
    """
    headers = {"Content-Type": "application/json"}
    url += f"?state='unsubmitted'"
    if community:
        url += f"&community={community_id}"
    r = requests.get(url, params={'access_token': token},
                     headers=headers)
    #print(r.url)
    #print(r.status_code)
    return r.json()


def remove_record(url, token, record_id, safe):
    """
    """

    headers = {"Content-Type": "application/json"}
    # if safe mode ask for confirmation before deleting 
    answer = 'Y'
    if safe:
        answer = input(f"Are you sure you want to delete {record_id}? (Y/N)")
    if answer == 'Y':
        r = requests.delete(url+ f"/{record_id}",
                params={'access_token': token},
                headers=headers)
        if r.status_code == 204:
            print('Record deleted successfully')
        else:
            print(r.status_code)
            print(r.url)
    else:
        print('Skipping record')


def main():

    # define urls, input file  if removing records from sandbox or production
    # by default records are removed from Sandbox and confirmation is asked for each record
    # pass 'api' and 'unsafe' as arguments to change default behaviour
    sandbox = True
    safe = True
    community_id = 'clex-data'
    community = False
    url = 'https://sandbox.zenodo.org/api/deposit/depositions'
    # get list of arguments, check if 'api and 'unsafe' are in list and
    # assign all other arguments to record_ids list
    record_ids = sys.argv[1:]
    if len(record_ids) >= 1:
        if 'community' in sys.argv[1:]:
            community = True
            record_ids.remove('community')
        if 'api' in sys.argv[1:] :
            sandbox = False
            url = 'https://zenodo.org/api/deposit/depositions'
            community_id = 'arc-coe-clex-data'
            record_ids.remove('api')
        if 'unsafe' in sys.argv[1:]:
            safe = False
            record_ids.remove('unsafe')

    # get either sandbox or api token to connect
    token = get_token(sandbox)

    # delete selected drafts if list of id was passed or delete all drafts

    # remove all records in record_ids list
    # if no record_ids were passed as input get all draft records 
    # double check state is correct as query seemed to ignore this filter
    if len(record_ids) == 0:
        records = get_drafts(url, token, community_id=community_id, community=community)
        record_ids = [x['id'] for x in records if x['state'] == 'unsubmitted']

    for record_id in record_ids:
        status = remove_record(url, token, record_id, safe)


if __name__ == "__main__":
    main()
