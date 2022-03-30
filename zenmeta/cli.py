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
import sys
from util import (config_log, post_json, get_token, read_json,
                  get_bucket, get_records, output_mode, remove_record)
from zenodo import set_zenodo, process_zenodo_plan
from invenio import set_invenio, process_invenio_plan
# if this remain different from zenodo I should move it to invenio.py file
from exception import ZenException

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
@click.option('--zenodo', 'portal', is_flag=True, default=False, flag_value='zenodo',
        help="To interact with Zenodo, instead of default Invenio")
@click.option('--production', '-p', 'production', is_flag=True, default=False,
               help="access to production site instead of test environment")
@click.option('--community', '-c', 'community_id', default="",
               help="Community identifier, if passed query corresponding" +
                     "community, default is empty str")
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def zen(ctx, portal, production, community_id, debug):
    ctx.obj={}
    if portal is None:
        ctx.obj['portal'] = 'invenio' 
        ctx = set_invenio(ctx, production)
    else:
        ctx.obj['portal'] = portal 
        # can only be zenodo currently could chnage in future
        ctx = set_zenodo(ctx, production)
    ctx.obj['log'] = config_log()
    # set up a config depending on portal and production values
    ctx.obj['production'] = production
    ctx.obj['community_id'] = community_id
    # get either sandbox or api token to connect
    ctx.obj['token'] = get_token(ctx.obj['portal'], ctx.obj['production'])

    if debug:
        ctx.obj['log'].setLevel(logging.DEBUG)
    ctx.obj['log'].debug(f"Token: {ctx.obj['token']}") 
    ctx.obj['log'].debug(f"Portal: {ctx.obj['portal']}") 
    ctx.obj['log'].debug(f"Community: {ctx.obj['community_id']}") 
    ctx.obj['log'].debug(f"API url: {ctx.obj['url']}") 
    ctx.obj['log'].debug(f"Production: {ctx.obj['production']}") 


@zen.command(name='meta')
@click.option('--fname', '-f', multiple=False, help="JSON file " +
              "containing metadata records to upload")
@click.option('--version', is_flag=True, default=False,
               help="Create new version if record already exists")
@click.pass_context
def upload_meta(ctx, fname, version):
    """Upload metadata from a list of records in a json input file.

    If a record exists already is updated, otherwise creates a new one.
    If "version" option is passed, creates a new version for record.

    Parameters
    ----------
    ctx: dict
        Click context obj including api information 
    fname: str
        Input json filename containing records to upload
    version: bool, optional
        If True create a new version for any existing records in list

    Returns
    -------
    """

    token = ctx.obj['token']
    zen_log = ctx.obj['log']
    zen_log.info(f"Uploading metadata from {fname} to {ctx.obj['portal']},"
                 + f" production: {ctx.obj['production']}")


    # read data from input json file and process plans in file
    data = read_json(fname)
    # process data for each plan and post records returned by process_plan()
    for plan in data:
        if ctx.obj['portal'] == 'zenodo':
            zen_log.info(plan['metadata']['title'])
            record = process_zenodo_plan(plan, ctx.obj['community_id'])
        else:
            zen_log.info(plan['title'])
            record = process_invenio_plan(plan)
        r = post_json(ctx.obj['url'], token, record, zen_log)
        zen_log.debug(f"Request: {r.request}") 
        zen_log.debug(f"Request url: {r.url}") 
        zen_log.info(r.status_code) 
    return


@zen.command(name='remove')
@click.option('--ids', '-i', multiple=True, help="Record ids to remove")
@click.option('--draft',  is_flag=True, default=True, help="If True " +
    "(default) remove drafts, zenodo published record cannot be removed")
@click.pass_context
def delete_records(ctx, ids, draft):
    """Delete drafts records based on their ids

    If a list or record ids is not passed then delete all drafts record

    """

    # still to do:
    # create a custom function for invenio to remove record
    # if possible we should just have one in util for both api
    # same for get_drafts 
    # add safe parameter
    # add drafts/published option where possible currently only drafts are selected
    token = ctx.obj['token']
    zen_log = ctx.obj['log']
    if len(ids) == 0:
        if ctx.obj['portal'] == 'zenodo':
            records = get_zenodo_drafts(url, token, 
                      community_id=ctx.obj['community_id'],
                      user=user, draft=draft)
            # double check state is correct as query seemed to ignore this filter
            ids = [x['id'] for x in records if x['state'] == 'unsubmitted']
        else:
            records = get_invenio_drafts(ctx.obj['url'], token,
                      user=user, draft=draft)
            ids = [x['id'] for x in records]
        zen_log.debug(f'{ids}')

    zen_log.info(f"Removing records {ids} from {ctx.obj['portal']},"
                 + f" production: {ctx.obj['production']}")
    safe = True
    for record_id in ids:
        status = remove_record(ctx, record_id, safe)


@zen.command(name='upload')
@click.option('--id', '-i', 'record_id', multiple=False,
              help="Id of record to upload files to")
@click.option('--fname', '-f',  multiple=False, help="Name of text " +
              "file with paths of files to upload, 1 file x line")
@click.pass_context
def upload_files(ctx, record_id, fname):
    """Upload files to existing record
    """

    token = ctx.obj['token']
    zen_log = ctx.obj['log']
    # get either sandbox or api token to connect

    # get bucket_url for record
    bucket_url = get_bucket(ctx.obj['url'], token, record_id)

    #read file paths from file
    with open(fname) as f:
        file_paths = f.readlines()

    # upload all files to record, one by one
    for f in file_paths:
        zen_log.info(f"Uploading {f} ...")
        f = f.replace('\n','')
        status = upload_file(bucket_url, token, record_id, f)
        zen_log.info(f"Request status: {status}")


@zen.command(name='list')
@click.option('--ids', '-i', multiple=True, help="Record ids to list")
@click.option('--mode', '-m', multiple=False, type=click.Choice(
              ['biblio', 'bibtex', 'json', 'ids', 'datacite-json',
               'csl', 'zenodo', 'marc-xml', 'datacite-xml',
               'dublin-core']), default='json', help="Output format" )
@click.option('--user', '-u',  is_flag=True, default=False, help="If True " +
              "list only user records")
@click.option('--draft',  is_flag=True, default=False, help="If True " +
              "list drafts, default is False")
@click.pass_context
def list_records(ctx, ids, user, draft, mode):
    """List records based on input arguments
    """
    token = ctx.obj['token']
    url = ctx.obj['url']
    zen_log = ctx.obj['log']
    # if draft is used with invenio user is automatically True
    if ctx.obj['portal'] == "invenio" and draft:
        user = True
    zen_log.debug(f"Draft is {draft}")
    zen_log.debug(f"Output mode is {mode}")
    zen_log.debug(f"User is {user}")
    if len(ids) == 0:
        records = get_records(ctx, user=user, draft=draft, mode=mode)
    else:
        records = []
        for recid in ids:
            records.append( get_records(ctx, record_id = recid, 
                            user=user, draft=draft, mode=mode) )
        zen_log.debug(f'{ids}')
    if mode not in ['bibtex', 'biblio']:
        records = output_mode(ctx, records, mode, user=user, draft=draft)  
    print(records)

if __name__ == '__main__':
    zen()
