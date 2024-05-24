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
                                  "%Y-%m-%d %H:%M")
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


def write_json(data, fname='output.json'):
    """Write data to a json file  
        
    Parameters
    ----------
    data : json object
        The file content as a json object
    fname : str
        Json filename 

    Returns
    -------
    """

    try:
        with open(fname, 'w') as f:
            json.dump(data, f, indent = 3)
    except:
        ZenException(f"Check that {data} exists and it is an object compatible with json")
    return 


def read_xml(fname):
    """ Read a xml file and return content 
        
    Parameters
    ----------
    fname : str
        xml filename 

    Returns
    -------
    data : json object
        The file content as a json object
    """
# to do!!!
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
        The url to post to
    token: str
        The authentication token
    data : json object
        The file content as a json object
    log: obj
        The logging obj to send debug information

    Returns
    -------
    r : requests object
      The requests response object
    """

    log.debug(f"Post request url: {url}")
    headers = {"Content-Type": "application/json"}
    params = {'access_token': token}
    r = requests.post(url,
            params=params, json=data,
            headers=headers)
    if r.status_code >= 400:
        log.info(r.text)
    return r


def put_json(url, token, data, log):
    """ Post data to a json file
        
    Parameters
    ----------
    url : str
        The url to post to
    token: str
        The authentication token
    data : json object
        The file content as a json object
    log: obj
        The logging obj to send debug information

    Returns
    -------
    r : requests object
      The requests response object
    """

    log.debug(f"Post request url: {url}")
    headers = {"Content-Type": "application/json"}
    params = {'access_token': token}
    r = requests.put(url,
            params=params, json=data,
            headers=headers)
    if r.status_code >= 400:
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
        The id for record we  to upload files to

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


def extract_records(ctx, records, mode, lrids, user=False, draft=False):
    """Extract actual records from request response depending on
       selected mode 
        
    Parameters
    ----------
    ctx : Click Context obj
        Including base url and cite url, community_id, portal and token info
    records : dict
        The records returned by the requests response
    user : bool, optional 
        If True retrieve all records for the user
        (default False) 
    draft : bool, optional 
        If True then retrieve only draft records 
        (default False) 
    mode : str, optional 
        Define the kind of information to retrieve, default is complete records 
        (default='json')

    Returns
    -------
    records : list
        The extracted records as a list
    """
    # if user specified record ids to retrieve then records list is ready 
    # otherwise extract from ['hits']['hits']
    # if mode ids, retrieve only the records' ids
    if lrids!= 0:
        pass
    else:
        records = records['hits']['hits']
    if mode == 'ids':
        records = [f"{x['metadata']['title']}, {x['id']}" for x in records]
        #records = [x['id'] for x in records]
    return records 


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
    # For invenio
    # {"status": 406, "message": "Invalid 'Accept' header. Expected one of: application/json, application/vnd.inveniordm.v1+json, application/vnd.citationstyles.csl+json, application/vnd.datacite.datacite+json, application/vnd.datacite.datacite+xml, application/x-dc+xml, text/x-bibliography"}
    # Build request url based on:
    # if record_id is passed retrieve a specific record
    # elif user is passed get all records for that user
    # elif community is passed get all records for community
    # else get all records
    # if draft is True get drafts otherwise get published records
    # only the user query or a specific record return drafts
    # NB community currently works only for zenodo
    #if ctx.obj['community_id'] != "" or user is True:
    if user is True:
        url = ctx.obj['deposit']
    else:
        url = ctx.obj['url']
        if (mode == 'bibtex' and not record_id and ctx.obj['community_id'] == ""
            and ctx.obj['portal'] == "zenodo"):
            raise ZenException("This would retrieve all records in zenodo!!\n"+
                    "Select a community_id or user option to limit query")
    params = {'access_token': ctx.obj['token'], 'size': 100}
    if record_id:
        url = url + f"/{record_id}"
    elif ctx.obj['community_id'] != "":
        params['communities'] = f"{ctx.obj['community_id']}"
    elif ctx.obj['portal'] == "invenio" and user:
        url = url.replace("/records","/user/records")
    if draft:
        if record_id:
            url = url + "/draft"
        elif ctx.obj['portal'] == "invenio" and user:
            params['q'] = "is_published:false"
        elif ctx.obj['portal'] == "zenodo":
            params['status'] = "draft"
    # send request
    r = requests.get(url, params=params,
                     headers=headers[mode])
    ctx.obj['log'].debug(f"{headers[mode]}")
    ctx.obj['log'].debug(f"{params}")
    ctx.obj['log'].debug(f"Request status code: {r.status_code}")
    ctx.obj['log'].debug(f"Request url: {r.url}")
    if mode in ['json', 'datacite-json', 'csl', 'vnd.zenodo.v1+json', 'ids']:
        output = r.json()
        # should differentiate ids from others!!!
    else:
        output = r.text
    ctx.obj['log'].debug(f"Type of output returned: {type(output)}")
    return output

# https://zenodo.org/oai2d?verb=ListRecords&metadataPrefix=oai_datacite
# for communities
# https://zenodo.org/oai2d?verb=ListRecords&metadataPrefix=oai_datacite&set=user-cfa


def remove_record(ctx, record_id, safe):
    """
    """

    headers = {"Content-Type": "application/json"}
    # if safe mode ask for confirmation before deleting
    url = ctx.obj['url']+ f"/{record_id}"
    log = ctx.obj['log']
    if ctx.obj['portal'] == "invenio":
        url = url + "/draft"
    if safe:
        answer = input(f"Are you sure you want to delete {record_id}? (Y/N)")
    else:
        answer = 'Y'
    if answer == 'Y':
        r = requests.delete(url,
                params={'access_token': ctx.obj['token']},
                headers=headers)
        if r.status_code == 204:
            log.info("Record deleted successfully")
        else:
            log.info(f"Request status code: {r.status_code}")
            log.info(f"Request url: {r.url}")
    else:
        log.info("Skipping record")
        r = None
    return r


def convert_ror(affiliation):
    """Convert affiliation to ror  codes used by invenio

    Parameters
    ----------
    affiliation: str
        Affiliation name

    Returns
    -------
    ror: str
        ROR id as recorded in invenio affiliations vocabulary
    """
    rors = read_json("data/affiliations.json")
    if affiliation == "University of New South Wales":
        affiliation = "UNSW Sydney"
    # intialise ror in case there is no match
    ror = {'name': affiliation}
    for k,v in rors.items():
        if affiliation in k:
            ror = {'name': k, 'id': v['id']}
            break
    return ror


def convert_for(code08):
    """Convert ANZSRC FOR codes from 2008 to 2020 classification

    Parameters
    ----------
    code08: int/str 
        Code in FOR2008 style id (int) or name (str)

    Returns
    -------
    map_codes: list(dict)
        List of FOR2020 mappings (dictionaries) for input code
    """
    map_codes = []
    codes20 = read_json("data/for_map.json")
    if isinstance(code08, int):
        map_codes = codes20[code08['code']]['codes_2020']
    else:
        for k,v in codes20.items():
            if v['name_2008'] == code08:
                map_codes = v['codes_2020']
    return map_codes
