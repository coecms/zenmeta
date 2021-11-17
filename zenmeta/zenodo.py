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
from exception import ZenException


def set_zenodo(ctx, production):
    """Add Zenodo details: api urls, communities, to context object

    Parameters
    ----------
    ctx: dict
        Click context obj to pass arguments onto sub-commands
    production: bool
        If True using production api, if False using sandbox

    Returns
    -------
    ctx: dict
        Click context obj to pass arguments onto sub-commands
    """

    if production:
        base_url = 'https://zenodo.org/api'
    else:
        base_url = 'https://sandbox.zenodo.org/api'
    # removing this for the moment as this doesn't filter based on user
    #can lead to list all the zenodo if used without
    ctx.obj['url'] = f'{base_url}/records'
    ctx.obj['deposit'] = f'{base_url}/deposit/depositions'
    ctx.obj['community'] = '&community='
    return ctx


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


def get_bibtex(token, out, community_id=""):
    """Get published records list in selected format

    Parameters
    ----------
    token : str
        The authentication token for the api 
    out: str
        Output type, defines the headers to use in the request
    community : bool, optional 
        If True then retrieve all the draft records for the community
        (default False) 
    community_id : str, optional
        The community identifier to add to the url. It has to be present if community True
        (default None)

    Returns
    output: str/json depending on input 'out'
        The text/json request response
    """

    headers_dict = {'biblio': {"Content-Type": "text/x-bibliography"},
                    'bibtex': {"Content-Type": "application/x-bibtex"},
                    'json': {"Content-Type": "application/json"}
                    }
    test_url='https://sandbox.zenodo.org/api/deposit/depositions'
    api_url='https://zenodo.org/api/deposit/depositions'
    r = requests.get(api_url,
        params={'access_token': token, 'status': 'published',
                'communities': community_id},
                headers=headers[out])
    if out == 'json':
        output = r.json
    else:
        output = r.text
    return output


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

    bits = author['name'].split()
    firstname = " ".join(bits[:-1])
    surname = bits[-1]
    #try:
    #    firstname, surname = author['name'].split()
    #    author['name'] = f"{surname}, {firstname}" 
    #except:
    #    log.info(f"Could not process name {author['name']} because " +
    #             "there are more than 1 firstname or surname")
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


def process_zenodo_plan(plan, community_id):
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

def upload_meta(ctx, fname, auth_fname):

    # define urls, input file and if loading to sandbox or production
    global authors
    # read a list of already processed authors from file, if new authors are found this gets updated at end of process
    if auth_fname:
        authors = read_json(auth_fname)

    # get either sandbox or api token to connect
    token = get_token(ctx['production'])

    # read data from input json file and process plans in file
    data = read_json(fname)
    # process data for each plan and post records returned by process_plan()
    for plan in data:
        record = process_zenodo_plan(plan, ctx['community_id'])
        print(plan['metadata']['title'])
        r = post_json(ctx['url'], token, record)
        print(r.status_code)
    # optional dumping authors list
    with open('latest_authors.json', 'w') as fp:
        json.dump(authors, fp)
    return

