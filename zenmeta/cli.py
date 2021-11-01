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
import click
import logging
from datetime import date
from os.path import expanduser
import sys
from util import post_json, get_token, read_json
#from .exception import ZenException

def zen_catch():
    debug_logger = logging.getLogger('zen_debug')
    debug_logger.setLevel(logging.CRITICAL)
    try:
        zen()
    except Exception as e:
        click.echo('ERROR: %s'%e)
        debug_logger.exception(e)
        sys.exit(1)


@click.group()
@click.option('--sandbox', 'sanbox', is_flag=True, default=False,
               help="access the sandbox instead of zenodo")
@click.option('-c/--community', 'community', default=False,
               help="returns only local files matching arguments in local database")
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def zen(ctx, sandbox, community, debug):
    ctx.obj={}
    ctx.obj['log'] = config_log()
    # set up a config depending on sandbox/api
    ctx.obj['sandbox'] = sandbox
    if sandbox is False:
        ctx.obj['url'] = 'https://zenodo.org/api/deposit/depositions'
        ctx.obj['community_id'] = 'arc-coe-clex-data'
    else:
        ctx.obj['url'] = 'https://sandbox.zenodo.org/api/deposit/depositions'
        ctx.obj['community_id'] = 'clex-data'

    if debug:
        debug_logger = logging.getLogger('clef_debug')
        debug_logger.setLevel(logging.DEBUG)

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

def meta_args(f):
    """Define upload_meta arguments
    """
    constraints = [
        click.option('--input', '-i', 'fname', multiple=False,
                      help="JSON file containing metadata records to upload"),
        click.option('--authors', 'auth_fname', multiple=False, default='authors.json',
                      help="Optional json file containing a list of authors already in accepted format"),
        ]
    for c in reversed(constraints):
        f = c(f)
    return f

@zen.command()
@meta_args
@click.pass_context
def upload_meta(ctx, fname, auth_fname):

    # define urls, input file and if loading to sandbox or production
    global authors
    # read a list of already processed authors from file, if new authors are found this gets updated at end of process
    if auth_fname:
        authors = read_json(auth_fname) 

    # get either sandbox or api token to connect
    token = get_token(ctx['sandbox'])

    # read data from input json file and process plans in file
    data = read_json(fname)
    # process data for each plan and post records returned by process_plan()
    for plan in data:
        record = process_plan(plan, ctx['community_id'])
        print(plan['metadata']['title'])
        r = post_json(ctx['url'], token, record)
        print(r.status_code)
    # optional dumping authors list
    with open('latest_authors.json', 'w') as fp:
        json.dump(authors, fp)
    return
