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
import csv
from datetime import date
from os.path import expanduser
from util import convert_for, convert_ror
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
        base_url = "https://zenodo.org/api"
    else:
        base_url = "https://sandbox.zenodo.org/api"
    # removing this for the moment as this doesn't filter based on user
    #can lead to list all the zenodo if used without
    ctx.obj['url'] = f"{base_url}/records"
    ctx.obj['deposit'] = f"{base_url}/deposit/depositions"
    ctx.obj['community'] = "&communities="
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

    headers = {'Content-Type': "application/octet-stream"}
    with open(fpath, 'rb') as fp:
        r = requests.put(
            f"{bucket_url}/{fpath}",
            data=fp,
            params={'access_token': token},
            headers=headers)
    return r


def process_author(author):
    """Create a author dictionary following the Zenodo api requirements 
        
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
    ind = license.find("Attribution")
    if ind == -1:
         print("check license for this record")
         zlicense = {'id': "cc-by-4.0"}
    else:
        zlicense = {'id': license[0:ind].strip().replace(" ","-").lower() + "-4.0"}
    return zlicense


def process_related_id(plan):
    """Add plan records and other references as references

    Parameters
    ----------
    plan: dict 
        Click context obj to pass arguments onto sub-commands

    Returns
    -------

    """
    rids = []
    relationship = {'geonetwork': "isReferencedBy", 'rda': "isAlternateIdentifier",
                      'related': "describes"}
    for k in ["geonetwork", "rda"]:
        if any(x in plan[k] for x in ["http://", "https://"]):
            rids.append({'identifier': plan[k], 'relation': relationship[k]}) 
    for rel in plan['related']:
        if any(x in rel for x in ["http://", "https://"]):
            rids.append({'identifier': rel, 'relation': relationship['related']}) 
    return rids


def process_keywords(keywords):
    """Get keywords string and return list""" 
    keys = keywords.split(",")
    return keys


def process_zenodo_plan(plan, community_id):
    """
    Parameters
    ----------
    ctx: dict
        Click context obj to pass arguments onto sub-commands
    production: bool
        If True using production api, if False using sandbox

    Returns
    -------

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
    if "keywords" in metadata.keys():
        metadata['keywords'] = process_keywords(plan['keywords'])
    metadata['notes'] = "Preferred citation:\n" + plan['citation'] + \
        "\n\nAccess to the data is via the NCI geonetwork record in related" + \
        " identifiers, details are also provided in the readme file."
    #metadata['doi'] = '/'.join(plan['doi'].split('/')[-2:])
    metadata['title'] = plan['title']
    metadata['version'] = plan['version'] 
    metadata['description'] = plan['description']
    metadata['upload_type'] = "dataset"
    metadata['language'] = "eng"
    metadata['access_right'] = "open"
    metadata['communities'] = [{'identifier': community_id}]
    #metadata['publication_date'] = date.today().strftime("%Y-%m-%d"),
    final = {}
    final['metadata'] = metadata
    final['modified'] = date.today().strftime("%Y-%m-%d")
    final['state'] = "inprogress"
    final['submitted'] = False
    return final 


def upload_meta(ctx, fname, auth_fname):
    """Upload metadata plan to zenodo

    Parameters
    ----------
    ctx: dict
        Click context obj to pass arguments onto sub-commands
    fname: str
        Name of json file containing plan
    auth_fname: str
        Name of json file containing authors mapping

    Returns
    -------
    """

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
    with open("latest_authors.json", 'w') as fp:
        json.dump(authors, fp)
    return


def map_identifiers(idlist):
    """Map identifiers from zenodo to invenio style 

    Parameters
    ----------
    idlist: list
        List of current zenodo identifiers
    
    Returns
    -------
    rel_ids: list
        List of invenio related identifiers
    alt_ids: list
        List of invenio alternate identifiers


    """
    rel_ids = []
    #alt_ids = []
    for rid in idlist:
        #if rid['relation'] == "isAlternateIdentifier":
        #    rid.pop('relation')
        #    alt_ids.append(rid)
        #else:
            rel_ids.append( {'identifier': rid['identifier'],
                'relation_type': {'id': rid['relation'].lower()},
                'resource_type': {'id': "other-resource", 'title': {'en': "Other"}},
                'scheme': rid['scheme']} )
    #return rel_ids, alt_ids
    return rel_ids


def invenio_creator(record):
    """To convert a creator record from zenodo to invenio style 

        Parameters
    ----------
    record: dict
        The creator record from a zenodo plan

    Returns
    -------
    new: dict
        The creator record formatted for invenio

    """
    new ={}
    try:
        surname, name = record['name'].split(",")
    except:
        surname, name = record['name'], ""
    new['affiliations'] = [ convert_ror(record.get('affiliation', "NONE")) ]
    new['person_or_org'] = { 'family_name': surname,
        'given_name': name, 'identifiers': [
        {'identifier': record.get('orcid', ""), 'scheme': "orcid"}],
        'name': record['name'], 'type': "personal" }
    return new

def invenio_license(license):
    """
    """
    licenses = [] 
    with open('data/licenses.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=";")
        for row in reader:
            licenses.append(row[0])
    if license.lower() in licenses:
        right = {'id': license.lower()} 
    else:
        right = { 'description': {'en': license},
                  'title': {'en': "Custom license"} }
    return [right]

    

def to_invenio(plan):
    """Convert a zenodo plan to an invenio style plan

    Parameters
    ----------
    plan: dict
        Original zenodo plan downloaded as json

    Returns
    -------
    meta: dict
        Metadata dictionary for Invenio style plan

    """

    # Most of the plan can be used as it is, only selected elements need to be mapped
    meta = plan['metadata']
    # if access restricted skip entire record
    #if meta['access_right'] == "restricted":
    #    return {} 
    # remove unwanted keys
    #discard = ['access_right_category', 'prereserve_doi']
    #[meta.pop(k, None) for k in discard]

    # upload_type _> resource_type
    #rtype = meta.pop('upload_type')
    #meta['resource_type'] = {'id': rtype, 'title': rtype.capitalize()}
    # notes -> additional_information
    #notes = meta.pop('notes', "")
    #meta['additional_descriptions'] = { 'description': notes,
    #    'lang': { 'id': "eng", 'title': {'en': "English"} },
    #    'type': { 'id': "citation-access",
    #              'title': {'en': "Citation and access information"} }
    #    }

    # try to guess publisher
    #if "10.4225" in meta['doi']:
    #    meta['publisher'] = "NCI Australia"
    #elif "zenodo" in meta['doi']:
    #    meta['publisher'] = "Zenodo"

    # fix existing related_identifiers
    #meta['related_identifiers'], meta['identifiers'] = map_identifiers(meta['related_identifiers'])
    rel_ids = meta.pop('related_identifiers', [])
    #meta['related_identifiers'] = map_identifiers(rel_ids)
    meta['identifiers'] = meta.pop('alternate_identifiers', [])
    # extend related identifiers
    # zenodo url -> related_identifiers
    url = plan['links']['parent_html']
    rel_ids.append( {'identifier': url,
         'relation_type': {'id': "ismetadatafor", 'title': {'en': "Is Metadata for"} },
         'resource_type': {'id': "metadata", 'title': {'en': "Metadata record"}},
         'scheme': "url"} )
    # communities -> related_identifiers
    recognised_coms = ["arc-coe-clex-data", "arc-coe-clex"]
    communities = meta.pop('communities', [])
    if communities != []: 
        identifiers = [c['id'] for c in communities if c['id'] in recognised_coms]
        for comid in identifiers:
            rel_ids.append( {'identifier': f"https://zenodo.org/communities/{comid}",
                   'relation_type': {'id': "ispartof", 'title': {'en': "Is part of"} },
                   'resource_type': {'id': "service-portal", 'title': {'en': "Data portal"}},
                   'scheme': "url"} )
    # grants  -> grants?? related_identifiers
    # grants will be eventually a field in itslef for the moment add to related_identifiers
    grants = meta.pop('grants', [])
    if grants != []: 
        for g in grants:
            gid = f"{g['funder']['doi']}::{g['code']}"
        identifiers = [g['id'] for g in grants]
        for gid in identifiers:
            rel_ids.append( {'identifier': f"{gid}",
                   'relation_type': {'id': "isrelatedto", 'title': {'en': "Is related to"} },
                   'resource_type': {'id': "other-resource", 'title': {'en': "Other"}},
                   'scheme': "doi"} )
    meta['related_identifiers'] = rel_ids

    # references -> description
    ##references = meta.pop('references', [])
    #for ref in references:
    #    meta['description'] += f"{ref}\n"

    # license -> rights 
    #license = meta.pop('license', {})
    #meta['rights'] = invenio_license(license['id'])

    # keywords  -> subjects and add to description for double checking
    # first work out if they could be for codes
    keywords = meta['subjects']
    codes20 = []
    for k in keywords:
        codes20.extend( convert_for(k) )
    meta['subjects'] = []
    for c in codes20:
        meta['subjects'].append({ 'scheme': "ANZSRC-FOR",
                                  'id': c['code'], 
                                 'subject': c['name']})
    keystr = f"<p>Keywords: {', '.join([x['subject'] for x in keywords])}</p>"
    meta['description'] += keystr

    # language -> languages
    #meta['languages'] =   {'id': "eng", 'title': {'en': "English"}}
    #meta.pop('language', None)

    # creators
    #creators = meta.pop('creators')
    #meta['creators'] = []
    #for record in creators:
    #    meta['creators'].append( invenio_creator(record) )

    final = {}
    final['metadata'] = meta
    #final['modified'] = date.today().strftime("%Y-%m-%d")
    final['files'] = {'enabled': False, 'order': []}
    final['access'] = {'record': "public", 'files': "public", 'status': "metadata-only",
                       'embargo': {'active': False, 'reason': None} }
    doi = plan['pids']['doi'].get('identifier',None)
    if not (doi and doi.strip()):
        doi = "10.1234567/" + random_string()
    final['pids'] = {'doi': {'identifier': doi,
                                 'provider': "external"}}

    print(final)
    return final 


