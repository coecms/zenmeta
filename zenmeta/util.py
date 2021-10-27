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

def get_token(sandbox=False):
    """ Read api authentication token for zenodo or sandbox 
        
    Parameters
    ----------
    sandox : bool, optional
        If True read and return the sandbox token (default is
        False)

    Returns
    -------
    token : str
        The authentication token for the zenodo or sandbox api
    """

    if sandbox:
        fname = expanduser('~/.sandbox')
    else:
        fname = expanduser('~/.zenodo')
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
        print(f"Check that {fname} is a proper json file")
        sys.exit()
    return data


def post_json(url, token, data):
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

    headers = {"Content-Type": "application/json"}
    r = requests.post(url,
            params={'access_token': token}, json=data,
            headers=headers)
    if r.status_code in  [400, 403, 500]:
        print(r.text)
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


def get_drafts(url, token, community_id=None, community=False):
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
        print('You need to pass the community_id to retrieve drafts from a community')
        sys.exit()

    headers = {"Content-Type": "application/json"}
    url += f"?state='unsubmitted'"
    if community:
        url += f"&community={community_id}"
    r = requests.get(url, params={'access_token': token},
                     headers=headers)
    #print(r.url)
    #print(r.status_code)
    drafts = r.json()
    return drafts


def process_author(author):
    """ Create a author dictionary following the api requirements 
        
    Parameters
    ----------
    author : dict 
        The author details 

    Returns
    -------
    author : dict
        A modified version of the author dictionary following the api requirements
    """

    try:
        firstname, surname = author['name'].split()
        author['name'] = f"{surname}, {firstname}" 
    except:
        print(f"Could not process name {author['name']} because there are more than 1 firstname or surname")
    author['orcid'] = author['orcid'].split("/")[-1]
    author['affiliation'] = ""
    author.pop('email')
    return author


def process_license(license):
    """If license is Creative Common return the zenodo style string
       else return license as in plan 
        
    Parameters
    ----------
    license : dict 
        A string defining the license

    Returns
    -------
    zlicense : dict
        A modified version of the license dictionary following the api requirements
    """

    # not doing yet what it claims
    ind = license.find('Attribution')
    if ind == -1:
         print('check license for this record')
         zlicense = {'id': 'cc-by-4.0'}
    else:
        zlicense = {'id': license[0:ind].strip().replace(' ','-').lower() + '-4.0'}
    return zlicense


def process_related_id(plan):
    """Add plan records and other references as references
    """
    rids = []
    relationship = {'geonetwork': 'isReferencedBy', 'rda': 'isAlternateIdentifier',
                      'related': 'describes'}
    for k in ['geonetwork','rda']:
        if any(x in plan[k] for x in ['http://', 'https://']):
            rids.append({'identifier': plan[k], 'relation': relationship[k]}) 
    for rel in plan['related']:
        if any(x in rel for x in ['http://', 'https://']):
            rids.append({'identifier': rel, 'relation': relationship['related']}) 
    return rids


def process_keywords(keywords):
    """Add plan records and other references as references
    """
    keys = keywords.split(",")
    return keys


def process_plan(plan, community_id):
    """
    """
    global authors
    metadata = {}
    if plan['author']['name'] in authors.keys():
        metadata['creators'] = authors[plan['author']['name']]
    else:
        metadata['creators'] = [process_author(plan['author'])]
        authors[plan['author']['name']] = metadata['creators']
    metadata['license'] = process_license(plan['license'])
    metadata['related_identifiers'] = process_related_id(plan)
    if 'keywords' in metadata.keys():
        metadata['keywords'] = process_keywords(plan['keywords'])
    metadata['notes'] = 'Preferred citation:\n' + plan['citation'] + \
                       "\n\nAccess to the data is via the NCI geonetwork record in related identifiers, details are also provided in the readme file."
    #metadata['doi'] = '/'.join(plan['doi'].split('/')[-2:])
    metadata['title'] = plan['title']
    metadata['version'] = plan['version'] 
    metadata['description'] = plan['description']

    metadata['upload_type'] = 'dataset'
    metadata['language'] = 'eng'
    metadata['access_right'] = 'open'
    metadata['communities'] = [{'identifier': community_id}]
    #metadata['publication_date'] = date.today().strftime("%Y-%m-%d"),
    final = {}
    final['metadata'] = metadata
    final['modified'] = date.today().strftime("%Y-%m-%d")
    final['state'] = 'inprogress'
    final['submitted'] = False
    return final 
