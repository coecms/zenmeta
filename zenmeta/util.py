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


def get_invenio_drafts(url, token, community_id=None, community=False):
    """Get a list of yours or all drafts records for a specific community

    Parameters
    ----------
    url : str
        The url address to post to 
    token : str
        The authentication token for the api 
    community : bool, optional 
        If True then retrieve all the draft records for the community
        (default False) 
    community_id : str, optional
        The community identifier to add to the url. It has to be present if community True
        (default None)

    Returns
    -------
    drafts : json object
        A list of all the draft record_ids returned by the api query  
    """
    
    # potentially consider this
    #get_drafts(url, token, record_id=None, community_id=None, community=False):
    #if record_id:
    #    url += f"${record_id}"
    #else:
    #    url += f"?state='unsubmitted'"
    #    if all:
    if community and community_id is None:
        ZenException('Missing community_id')

    headers = {"Content-Type": "application/json"}
    url += f"?state='unsubmitted'"
    if community:
        url += f"&community={community_id}"
    r = requests.get(url, params={'access_token': token},
                     headers=headers)
    log.debug(f"Request status code: {r.status_code}")
    log.debug(f"Request url: {r.url}")
    drafts = r.json()
    return drafts


def remove_record(url, token, record_id, safe, log):
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
            log.info("Record deleted successfully")
        else:
            log.info(f"Request status code: {r.status_code}")
            log.info(f"Request url: {r.url}")
    else:
        log.info("Skipping record")

