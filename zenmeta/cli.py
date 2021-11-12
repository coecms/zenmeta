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
                  bucket_url)
from zenodo import set_zenodo, process_zenodo_plan, get_zenodo_drafts
from invenio import set_invenio, process_invenio_plan
# if this remain different from zenodo I should move it to invenio.py file
from util import get_invenio_drafts
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
@click.option('--zenodo', 'portal', is_flag=True, default=False, flag_value='zenodo',
        help="To interact with Zenodo, instead of default Invenio")
@click.option('--production', 'production', is_flag=True, default=False,
               help="access to production site instead of test environment")
@click.option('-c/--community', 'community', default=False,
               help="returns only local files matching arguments in local database")
@click.option('--debug', is_flag=True, default=False,
               help="Show debug info")
@click.pass_context
def zen(ctx, portal, production, community, debug):
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
    # get either sandbox or api token to connect
    ctx.obj['token'] = get_token(ctx.obj['portal'], ctx.obj['production'])

    if debug:
        ctx.obj['log'].setLevel(logging.DEBUG)
    ctx.obj['log'].debug(f"Token: {ctx.obj['token']}") 


def meta_args(f):
    """Define upload_meta arguments
    """
    constraints = [
        click.option('--authors', 'auth_fname', multiple=False, default='authors.json',
                      help="Optional json file containing a list of authors already in accepted format"),
        ]
    for c in reversed(constraints):
        f = c(f)
    return f


@zen.command()
@click.option('--fname', '-f', multiple=False, help="JSON file " +
              "containing metadata records to upload")
@click.pass_context
def upload_meta(ctx, fname, auth_fname):
    """Upload metadata from a list of records in a json input file

    Parameters
    ----------
    ctx: dict
        Click context obj including api information 
    fname: str
        Input json filename containing records to upload

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
            draft_upload = ''
        else:
            zen_log.info(plan['title'])
            record = process_invenio_plan(plan)
            draft_upload = '/records'
        r = post_json(ctx.obj['url']+draft_upload, token, record, zen_log)
        zen_log.debug(f"Request: {r.request}") 
        zen_log.debug(f"Request url: {r.url}") 
        zen_log.info(r.status_code) 
    return


@zen.command()
@click.option('--ids', '-i', multiple=True, help="Record ids to remove")
@click.option('--drafts',  is_flag=True, default=True, help="If True " +
    "(default) remove drafts, zenodo published record cannot be removed")
@click.pass_context
def delete_records(ctx, ids, drafts):
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
            records = get_zenodo_drafts(url, token, community_id=community_id, community=community)
            # double check state is correct as query seemed to ignore this filter
            ids = [x['id'] for x in records if x['state'] == 'unsubmitted']
        else:
            records = get_invenio_drafts(url+get_user_records, token)
            ids = [x['id'] for x in records]
        zen_log.debug(f'{ids}')

    zen_log.info(f"Removing records {ids} from {ctx.obj['portal']},"
                 + f" production: {ctx.obj['production']}")
    if ctx.obj['portal'] == 'zenodo':
        safe = True
        for record_id in ids:
            status = remove_zenodo_record(ctx.obj['url'], token, record_id, safe)
    else:
        for recid in ids:
            newurl = ctx.obj['url'] + f'/records/{recid}/draft' + f'?access_token={token}'
            zen_log.debug(newurl)
            r = requests.delete(newurl)
            zen_log.info(r)
            zen_log.debug(r.text)


@zen.command()
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


if __name__ == '__main__':
    zen()
