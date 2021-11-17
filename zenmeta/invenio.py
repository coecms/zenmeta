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


def process_contributor(contrib, roles):
    """Rewrite authors and collaborators information following metadata scheme 
       Currently assumes input format is one generated by scraping geonetwork with 
       name, affiliation, role and org (True/False) attributes.

    Parameters
    ----------
    creator : dict 
        The contributor details 
    roles : dict 
        Dictionary mapping geonetwork (iso19115) roles to invenio (datacite) ones

    Returns
    -------
    contributor : dict
        A modified version of the author dictionary
    """
    creator = {}
    creator['affiliation'] = {'id': "", 'name': contrib['affiliation']}
    # assign role based on mapping dictionary
    creator['role'] = roles[contrib['role']]
    if contrib['org'] == False:
        bits = contrib['name'].split()
        firstname = " ".join(bits[:-1])
        surname = bits[-1] 
        creator['person_or_org'] = { 'family_name': surname,
                'given_name' : firstname,
                'name' : f"{surname}, {firstname}",
                'type' : 'personal',
                #'identifier' : [{'scheme': 'orcid', 'identifier': contrib['orcid'].split("/")[-1]}]
                }
    else:
        creator['person_or_org'] = { 'name': contrib['name'],
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
                    "id": "isoriginalformof", "title": {"en": "Is original form of"} },
                    "resource_type": {"id": "other-resource", "title": {"en": "Other"}},
                    "scheme": "url"} )
            else: 
                related.append( {"identifier": v, "relation_type": {
                    "id": "describes", "title": {"en": "Describes"} },
                    "resource_type": {"id": "other-resource", "title": {"en": "Other"}},
                    "scheme": "url"} )
    return related


def process_invenio_plan(plan):
    """
    """
    metadata = {}
    # Contributors
    roles = read_json("data/CI_RoleCode.json")
    metadata['creators'] = []
    metadata['collaborators'] = []
    for p in plan['contributors']:
        contributor = process_contributor(p, roles) 
        if p['role'] in ['author']:
            metadata['creators'].append( contributor )
        else:
            metadata['collaborators'].append( contributor )
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
    # version not abvailable from geonetwork
    #metadata['version'] = plan['version']
    metadata['description'] = plan['description']
    # License, additional descriptions
    if plan['license'] != "":
        metadata['rights'] = [ process_license(plan['license']) ]
    metadata['additional_descriptions'] = add_description(plan['citation'], plan['location'])

    # Geospatial info: build GeoJSON polygon
    if plan['geospatial'] not in [[], ""]:
        metadata['locations'] = process_spatial(plan['geospatial'])

    #if 'subjects' in plans.keys():
    #    metadata['subjects'] = process_subjects(plan['subjects'])
    # Related identifiers and identifiers
    metadata['related_identifiers'] = process_links(plan['links'])
    if plan['doi'] != "":
        metadata['identifiers'] = [{'identifier': plan['doi'], 'scheme': "doi"}]
    metadata['resource_type'] = {'id': 'dataset', 'title': "Dataset"}
    metadata['language'] = 'English'
    metadata['publisher'] = 'NCI Australia'
    final = {}
    final['metadata'] = metadata
    #final['modified'] = date.today().strftime("%Y-%m-%d")
    final['files'] = {'enabled': False, "order": []}
    final['access'] = {'record': "public", 'files': "public", 'status': "metadata-only",
                       'embargo': {'active': False, 'reason': None} }
    return final
