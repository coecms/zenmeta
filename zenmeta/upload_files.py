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


def upload_file(bucket_url, token, record_id, fpath):
    """Upload file to selected record

    Parameters
    ----------
    bucket_url : str
        The url for the file bucket to which upload files
    token : str
        The authentication token for the zenodo or sandbox api
    record_id : str
        The id for record we want to upload files to
    fpath : str
        The path for file to upload

    Returns
    -------
    r : requests object
      The requests response object

    """

    headers = {"Content-Type": "application/octet-stream"}
    with open(fpath, "rb") as fp:
        r = requests.put(
            f"{bucket_url}/{fpath}",
            data=fp,
            params={'access_token': token},
            headers=headers)
    return r


def main():

    # define urls, input file  if removing records from sandbox or production
    # by default records are removed from Sandbox and confirmation is asked for each record
    # pass 'api' and 'unsafe' as arguments to change default behaviour
    sandbox = True
    community_id = 'clex-data'
    community = False
    url = 'https://sandbox.zenodo.org/api/deposit/depositions/'
    # get list of arguments,
    # first should be list of files to upload,
    # check if 'api and 'unsafe' are in list and
    # assign the last value as record_id, if more than one the first will be sleected
    filelist = sys.argv[1]
    inputs = sys.argv[2:]
    if len(inputs) >= 1:
        if 'community' in sys.argv[1:]:
            community = True
            inputs.remove('community')
        if 'api' in sys.argv[1:] :
            sandbox = False
            url = 'https://zenodo.org/api/deposit/depositions/'
            community_id = 'arc-coe-clex-data'
            inputs.remove('api')

    record_id = inputs[0]
    # get either sandbox or api token to connect
    token = get_token(sandbox)

    # delete selected drafts if list of id was passed or delete all drafts

    # get bucket_url for record 
    bucket_url = get_bucket(url, token, record_id)

    #read file paths from file
    with open(filelist) as f:
        file_paths = f.readlines()

    # upload all files to record, one by one
    for f in file_paths:
        print(f)
        f = f.replace('\n','')
        status = upload_file(bucket_url, token, record_id, f)


if __name__ == "__main__":
    main()
