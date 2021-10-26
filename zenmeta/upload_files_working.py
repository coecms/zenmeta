import requests
import json
from datetime import date
from os.path import expanduser
import sys

def get_token(sandbox):
    """
    """
    if sandbox:
        fname = expanduser('~/.sandbox')
    else:
        fname = expanduser('~/.zenodo')
    with open(fname,'r') as f:
         token=f.readline().replace("\n","")
    return token


def read_json(fname):
    """
    """
    try:
        with open(fname, 'r') as f:
            data = json.load(f)
    except:
        print(f"Check that {fname} is a proper json file")
        sys.exit()
    return data


def get_drafts(url, token, record_id=None, community=False, community_id=None):
    """
    """
    headers = {"Content-Type": "application/json"}
    if record_id:
        url += f"${record_id}"
    else:
        url += f"?state='unsubmitted'"
        if community:
            url += f"&community={community_id}"
    r = requests.get(url, params={'access_token': token},
                     headers=headers)
    return r.json()


def upload_file(url, token, record_id, safe):
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
            print('Record deleted successfully')
        else:
            print(r.status_code)
            print(r.url)
    else:
        print('Skipping record')


def main():

    # define urls, input file  if removing records from sandbox or production
    # by default records are removed from Sandbox and confirmation is asked for each record
    # pass 'api' and 'unsafe' as arguments to change default behaviour
    sandbox = True
    safe = True
    community_id = 'clex-data'
    community = False
    url = 'https://sandbox.zenodo.org/api/deposit/depositions'
    # get list of arguments, check if 'api and 'unsafe' are in list and
    # assign all other arguments to record_ids list
    record_ids = sys.argv[1:]
    if len(record_ids) >= 1:
        if 'community' in sys.argv[1:]:
            community = True
            record_ids.remove('community')
        if 'api' in sys.argv[1:] :
            sandbox = False
            url = 'https://zenodo.org/api/deposit/depositions'
            community_id = 'arc-coe-clex-data'
            record_ids.remove('api')
        if 'unsafe' in sys.argv[1:]:
            safe = False
            record_ids.remove('unsafe')

    # get either sandbox or api token to connect
    token = get_token(sandbox)

    # delete selected drafts if list of id was passed or delete all drafts

    # remove all records in record_ids list
    # if no record_ids were passed as input get all draft records 
    # double check state is correct as query seemed to ignore this filter
    if len(record_ids) == 0:
        records = get_drafts(url, token, record_id, community, community_id)
        record_ids = [x['id'] for x in records if x['state'] == 'unsubmitted']

    for record_id in record_ids:
        status = remove_record(url, token, record_id, safe)


if __name__ == "__main__":
    main()
