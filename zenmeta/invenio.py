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
from util import post_json, get_token, read_json


def set_invenio(ctx, production):
    """Add Invenio details: api urls, communities, to context object

    Parameters
    ----------
    ctx: click context obj 
        Api details
    production: bool
        If True using production api, if False using sandbox

    Returns
    -------
    ctx: click context obj 
        Api details with url and community added 
    """

    if production:
        base_url = 'https://oneclimate.dmponline.cloud.edu.au/api'
    else:
        base_url = 'https://test.dmponline.cloud.edu.au/api'
    ctx.obj['url'] = f'{base_url}/records'
    ctx.obj['deposit'] = f'{base_url}/records'
    ctx.obj['communities'] = f'{base_url}/communities'
    return ctx


def process_party(party, roles):
    """Rewrite authors and contributors formatting following metadata scheme 
       Currently assumes input format is one generated by scraping geonetwork with 
       name, affiliation, role and org (True/False) attributes.

    Parameters
    ----------
    creator : dict 
        The party details 
    roles : dict 
        Dictionary mapping geonetwork (iso19115) roles to invenio (datacite) ones

    Returns
    -------
    party : dict
        A modified version of the author dictionary
    """
    creator = {}
    # open affiliations vocab to find id for institution
    aff_dict = read_json('data/affiliations.json')
    aff = party['affiliation']
    # try to find affiliation name in key dictionary, if not try in 
    aff_id = ""
    try:
        aff_id = aff_dict[aff]['id']
    except:
        for k,v in aff_dict.items():
            if v['acronym'] == aff:
                aff_id = v['id']
                aff = k
    if aff_id != "":
        creator['affiliations'] = [{'id': aff_id, 'name': aff}]
    # assign role based on mapping dictionary
    creator['role'] = roles[party['role']]
    if party['org'] == False:
        bits = party['name'].split()
        firstname = " ".join(bits[:-1])
        surname = bits[-1] 
        creator['person_or_org'] = { 'family_name': surname,
                'given_name' : firstname,
                'name' : f"{surname}, {firstname}",
                'type' : 'personal',
                #'identifier' : [{'scheme': 'orcid', 'identifier': party['orcid'].split("/")[-1]}]
                }
    else:
        creator['person_or_org'] = { 'name': party['name'],
        'type': "organizational"} 
    return creator 


def process_time(coverage):
    """Process time coverage range so it can be added to the dates metadata

    Parameters
    ----------
    coverage : list
        [From date, to date]

    Returns
    -------
    time_range : dict
        Dictionary following metadata schema for dates
    """

    time_range = {'date': f"{coverage[0]}/{coverage[1]}",
        'description': "Time range covered by data",
        'type': { 'id': "coverage", 'title': {'en': "Temporal coverage"} }
        }
    return time_range


def process_spatial(geo):
    """Process time range so it can be added to the dates metadata

    Parameters
    ----------
    geo : list
        [minLon, maxLon, minLat, maxLat]

    Returns
    -------
    polygon : dict(list(list))
        Dictionary following GeoJSON polygon format 
    """

    polygon = { "type": "Polygon",
      "coordinates": [
      [ [geo[0], geo[2]], [geo[0], geo[3]], [geo[1], geo[3]], [geo[1], geo[2]] ]
      ]}
    return polygon 


def add_description(citation, location):
    """Create metadata for additional descriptions,
       currently one for local location and one for preferred citation

    Parameters
    ----------
    citation : str
        The preferred citation
    location : str
        Information on hpe to access data locally

    Returns
    -------
    descriptions : list(dict)
        List with the two addtional description dictionaries
    """

    descriptions = [
        { "description": citation,
        "lang": { "id": "eng", "title": {"en": "English"} },
        "type": { "id": "citation-access",
                  "title": {"en": "Citation and access information"} }
        },
        { "description": location,
        "lang": { "id": "eng", "title": {"en": "English"} },
        "type": { "id": "location", "title": {"en": "Local host"} }
        }]
    return descriptions


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

    ind = license.find('Attribution')
    ind = license.find('creativecommons.org/licenses/')
    if ind == -1:
         zlicense = { "description": {"en": license},
                      "title": {"en": "Custom license"} }
    else:
        zlicense = {"id": "-".join(["cc", license[ind:].split("/")[2],"4.0"])}
    return zlicense


def process_links(links):
    """If license is Creative Common return the zenodo style string
       else return license as in plan

    Parameters
    ----------
    links : list(dict)
        A list of related identifiers

    Returns
    -------
    related : list(dict)
        A list of dictionaries of related links following the metadata schema 
    """

    related = []
    for link in links:
        for k,v in link.items():
            if k == 'geonetwork': 
                related.append( {"identifier": v, "relation_type": {
                    "id": "isderivedfrom", "title": {"en": "Is derived from"} },
                    "resource_type": {"id": "metadata", "title": {"en": "Metadata record"}},
                    "scheme": "url"} )
            elif k == 'RDA': 
                related.append( {"identifier": v, "relation_type": {
                    "id": "isvariantformof", "title": {"en": "Is variant form of"} },
                    "resource_type": {"id": "metadata", "title": {"en": "Metadata record"}},
                    "scheme": "url"} )
            elif k == 'TDS': 
                related.append( {"identifier": v, "relation_type": {
                    "id": "ismetadatafor", "title": {"en": "Is metadata for"} },
                    "resource_type": {"id": "dataset", "title": {"en": "Dataset"}},
                    "scheme": "url"} )
            elif k == 'paper': 
                related.append( {"identifier": v, "relation_type": {
                    "id": "iscitedby", "title": {"en": "Is cited by"} },
                    "resource_type": {"id": "publication-article", "title": {"en": "Journal article"}},
                    "scheme": "doi"} )
            else: 
                related.append( {"identifier": v, "relation_type": {
                    "id": "documents", "title": {"en": "Documents"} },
                    "resource_type": {"id": "other-resource", "title": {"en": "Other"}},
                    "scheme": "url"} )
    return related


def create_subject(scheme, sid, term):
    """
    """
    subject = { 'id': sid, 'scheme': scheme, 'subject': term}
    return subject 


def process_subjects(fformat, codes):
    """
    """
    subjects = []
    if fformat != "":
        fid, fterm = convert_format(fformat)
        fsub = create_subject('format', fid, fterm)
        subjects.append(fsub)
    if codes != []:
        for c in codes:
            csub = create_subject('ANZSRC-FOR', c['code'], c['name'])
            subjects.append(csub)
    return subjects


def convert_format(fformat):
    """
    """
    fformat = fformat.lower()
    if 'netcdf' in fformat:
        fid, term = 'netcdf', 'netcdf'
    elif 'grib' in fformat:
        fid, term = 'grib', 'grib'
    elif 'hdf' in fformat:
        fid, term = 'hdf', 'hdf'
    elif fformat == 'um':
        fid, term = 'binary', 'other binary'
    elif 'geotiff' in fformat:
        fid, term = 'geotiff', 'geotiff'
    elif 'mat' in fformat:
        fid, term = 'matlab', 'matlab'
    else:
        fid, term = 'other', 'other'
    return fid, term 


def process_parties(parties):
    """Process contributors for plan and separate them in authors and contributors
    """
    roles = read_json("data/CI_RoleCode.json")
    creators = []
    contributors = []
    for p in parties:
        party = process_party(p, roles) 
        if p['role'] in ['author']:
            creators.append( party )
        else:
            contributors.append( party )
    # make sure there's at least 1 author otherwise use owner/`rightsholder`
    if len(creators) == 0:
        i = 0
        for p in contributors:
            owners_ind = []
            if p['role']['id'] == 'rightsholder':
                # add it to creators
                creators.append(p)
                owners_ind.append(i)
            i+=1
        # now remove owners from contributors list
            [contributors.pop(i) for i in owners_ind]
    return creators, contributors


def process_invenio_plan(plan):
    """
    """
    metadata = {}

    # Contributors
    metadata['creators'] , metadata['contributors'] = process_parties(
                                                       plan['parties'])

    # Dates: publication date is required
    if plan['dates']['publication'] != "":
        metadata['publication_date'] = plan['dates']['publication']
    elif plan['dates']['creation'] != "":
        metadata['publication_date'] = plan['dates']['creation']
    else:
        metadata['publication_date'] = date.today().strftime('%Y-%m-%d') 
    metadata['dates'] = []
    if plan['time_coverage'] not in [[], ""]:
        metadata['dates'].append(process_time(plan['time_coverage']))
    # Titles, version, description
    metadata['title'] = plan['title']
    if plan['alt_title'] != "":
        metadata['additional_titles'] = [{"title": plan['alt_title'], 
            "type": {"id": "alternative-title", "title": {"en": "Alternative title"}}}]
    # version it's only sometime available from geonetwork
    if plan['version'] != "":
        metadata['version'] = plan['version']
    metadata['description'] = plan['description']
    # License, additional descriptions
    if plan['license'] != "":
        metadata['rights'] = [ process_license(plan['license']) ]
    metadata['additional_descriptions'] = add_description(plan['citation'], plan['location'])

    # Geospatial info: build GeoJSON polygon
    if plan['geospatial'] not in [[], ""]:
        metadata['locations'] = process_spatial(plan['geospatial'])

    # Related identifiers and identifiers
    metadata['related_identifiers'] = process_links(plan['links'])
    #if plan['doi'] != "":
    #    metadata['identifiers'] = [{'identifier': plan['doi'], 'scheme': "doi"}]

    # Create subjects based on fformat and for_codes
    metadata['subjects'] = process_subjects(plan['fformat'], plan['for_codes'])
    metadata['resource_type'] = {'id': 'dataset', 'title': "Dataset"}
    metadata['language'] = 'English'
    metadata['publisher'] = 'NCI Australia'
    final = {}
    final['metadata'] = metadata
    #final['modified'] = date.today().strftime("%Y-%m-%d")
    final['files'] = {'enabled': False, "order": []}
    final['access'] = {'record': "public", 'files': "public", 'status': "metadata-only",
                       'embargo': {'active': False, 'reason': None} }
    if plan['doi'] != "":
        final['pids'] = {'doi': {'identifier': plan['doi'], 
                         'client': 'external', 'provider': 'external'}}
    return final
