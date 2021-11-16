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
import logging
import os
import datetime as dt 
from bs4 import BeautifulSoup
from os.path import expanduser
from exception import ZenException


def config_log():
    """Configure log file to keep track of activity"""

    # start a logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logger = logging.getLogger('zen_log')
    # set a formatter to manage the output format of our handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  "%Y-%m-%d H:%M")
    # set the level for the logger, has to be logging.LEVEL not a string
    # until we do so cleflog doesn't have a level and inherits the root
    # logger level:WARNING
    logger.setLevel(logging.INFO)

    # add a handler to send WARNING level messages to console
    clog = logging.StreamHandler()
    clog.setLevel(logging.WARNING)
    logger.addHandler(clog)

    # add a handler to send DEBUG/INFO level messages to file
    # use DEBUG level for file handler, if main log level is INFO
    # only INFO messages will pass has this filter is applied first
    logname = expanduser(f'~/zenmeta_log.txt')

    flog = logging.FileHandler(logname)
    flog.setLevel(logging.DEBUG)
    flog.setFormatter(formatter)
    logger.addHandler(flog)

    # return the logger object
    return logger


def get_token(portal, production=False):
    """Read api authentication token for production/test api
        
    Parameters
    ----------
    portal: str
        The name of portal to access: zenodo/invenio
    production: bool, optional
        If True read and return the production token (default is False)

    Returns
    -------
    token : str
        The authentication token for the selected api
    """

    if production:
        fname = expanduser(f'~/.{portal}_production')
    else:
        fname = expanduser(f'~/.{portal}_test')
    with open(fname,'r') as f:
         token=f.readline().replace("\n","")
    return token


def output_mode(ctx, records, mode, user=False, draft=False):
    if ctx.obj['portal'] == "invenio":
        id_label = 'id'
        if user is False:
            records = records['hits']['hits']
    else:
        id_label = 'record_id'
    if mode == 'ids':
        records = [x[id_label] for x in records]
    return records 


def read_json(fname):
    """ Read a json file and return content 
        
    Parameters
    ----------
    fname : str
        Json filename 

    Returns
    -------
    data : json object
        The file content as a json object
    """

    try:
        with open(fname, 'r') as f:
            data = json.load(f)
    except:
        ZenException(f"Check that {fname} exists and it is a proper json file")
    return data


def read_xml(fname):
    """ Read a xml file and return content 
        
    Parameters
    ----------
    fname : str
        Json filename 

    Returns
    -------
    data : json object
        The file content as a json object
    """

    try:
        with open(fname, 'r') as f:
            data = json.load(f)
    except:
        ZenException(f"Check that {fname} exists and it is a proper json file")
    return data


def post_json(url, token, data, log):
    """ Post data to a json file
        
    Parameters
    ----------
    url : str
        The url address to post to 
    token : str
        The authentication token for the api 
    data : json object
        The file content as a json object

    Returns
    -------
    r : requests object
      The requests response object
    """

    log.debug(f"Post request url: {url}")
    headers = {"Content-Type": "application/json"}
    r = requests.post(url,
            params={'access_token': token}, json=data,
            headers=headers)
    if r.status_code in  [400, 403, 500]:
        log.info(r.text)
    return r


def get_bucket(url, token, record_id):
    """ Get bucket url from record json data
    
    Parameters
    ----------
    url : str 
        The base url of application
    token : str
        The authentication token for the api 
    record_id : str
        The id for record we want to upload files to

    Returns
    -------
    bucket_url : str
        The url for the file bucket to which upload files
    """

    headers = {"Content-Type": "application/json"}
    url += f"{record_id}"
    r = requests.get(url, params={'access_token': token},
                     headers=headers)
    return r.json()["links"]["bucket"]


def get_records(ctx, record_id=None, user=False, draft=False, mode='json'):
    """Get a list of yours or all drafts records for a specific community

    Parameters
    ----------
    ctx : Click Context obj
        Including base url and cite url, community_id, portal and token info
    record_id : str, optional
        The record identifier if present retrieve only that record
        (default None)
    user : bool, optional 
        If True then retrieve all records for the user
        (default False) 
    draft : bool, optional 
        If True then retrieve only draft records 
        (default False) 
    mode : str, optional 
        Define the kind of information to retrieve, default is complete records 
        (default='json')

    Returns
    -------
    records : json object
        A list of all the draft record_ids returned by the api query  
    """
    
    # Set headers depending on expected output
    headers = {'biblio': {"Accept": "text/x-bibliography"},
               'bibtex': {"Accept": "application/x-bibtex"},
               'json': {"Content-Type": "application/json"},
               'ids': {"Content-Type": "application/json"},
               'datacite-json': {"Content-Type": "application/json"},
               'csl': {"Content-Type": "application/json"},
               'zenodo': {"Content-Type": "vnd.zenodo.v1+json"},
               'marc-xml': {"Content-Type": "application/marcxml+xml"},
               'datacite-xml': {"Content-Type": "application/x-datacite+xml"},
               'dublin-core': {"Content-Type": "application/x-dc+xml"}
              }
    # Build request url based on:
    # if record_id is passed retireve a specific record
    # elif user is passer get all record for that user
    # elif community is passed get all record for community
    # else get all record
    # if draft is True get drafts otherwise get published records
    # only the user query or a specific record return drafts
    # NB community currently works only for zenodo
    if mode in ['ids', 'zenodo', 'json']:
        url = ctx.obj['url']
    else:
        url = ctx.obj['cite']
        if (ctx.obj['portal'] == "zenodo" and user is False
            and ctx.obj['community_id'] == ""):
            raise ZenException("This would retrieve all records in zenodo!!\n"+
                    "Select a community_id or user option to limit query")
    params = {'access_token': ctx.obj['token']}
    if record_id:
        url = url + f"/${record_id}"
    elif user:
        url = url.replace("/records","/user/records")
    elif ctx.obj['community_id'] != "":
        params['community'] = f"{ctx.obj['community_id']}"
    if draft:
        if record_id:
            url = url + "/draft"
        elif user:
        #elif ctx.obj['portal'] == "invenio":
            #url = url + f"?state='unsubmitted'"
            #url = url + f"?q=is_published:false"
            params['q'] = "is_published:false"
        elif ctx.obj['portal'] == "zenodo":
            #params['submitted'] = False
            params['status'] = "draft"
            #url = url + f"?q=submitted:False"

    r = requests.get(url, params=params,
                     headers=headers[mode])
    ctx.obj['log'].debug(f"{headers[mode]}")
    ctx.obj['log'].debug(f"{params}")
    ctx.obj['log'].debug(f"Request status code: {r.status_code}")
    ctx.obj['log'].debug(f"Request url: {r.url}")
    if mode in ['json', 'datacite-json', 'csl', 'vnd.zenodo.v1+json', 'ids']:
        output = r.json()
    else:
        output = r.text
        print(output)
    return output

# https://zenodo.org/oai2d?verb=ListRecords&metadataPrefix=oai_datacite
# for communities
# https://zenodo.org/oai2d?verb=ListRecords&metadataPrefix=oai_datacite&set=user-cfa


def remove_record(ctx, record_id, safe):
    """
    """

    headers = {"Content-Type": "application/json"}
    # if safe mode ask for confirmation before deleting
    answer = 'Y'
    url = ctx.obj['url']+ f"/{record_id}"
    if ctx.obj['portal'] == "invenio":
        url = url + "/draft"
    if safe:
        answer = input(f"Are you sure you want to delete {record_id}? (Y/N)")
    if answer == 'Y':
        r = requests.delete(ctx.obj['url']+ f"/{record_id}",
                params={'access_token': ctx.obj['token']},
                headers=headers)
        if r.status_code == 204:
            log.info("Record deleted successfully")
        else:
            log.info(f"Request status code: {r.status_code}")
            log.info(f"Request url: {r.url}")
    else:
        log.info("Skipping record")
    return r

