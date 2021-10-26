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


def get_bucket(url, token, record_id):
    """ Get bucket url from record json data
        Input:
            url - (string) the base url of application
            token - the api token
            record_id - the id for record we want to upload files to
    """
    headers = {"Content-Type": "application/json"}
    url += f"{record_id}"
    print(url)
    r = requests.get(url, params={'access_token': token},
                     headers=headers)
    return r.json()["links"]["bucket"]


def upload_file(bucket_url, token, record_id, fpath):
    """Upload file to selected record
    """
    headers = {"Content-Type": "application/octet-stream"}
    with open(fpath, "rb") as fp:
        r = requests.put(
            f"{bucket_url}/{fpath}",
            data=fp,
            params={'access_token': token},
            headers=headers)
    print(r.json())
    return


def main():

    # define urls, input file  if removing records from sandbox or production
    # by default records are removed from Sandbox and confirmation is asked for each record
    # pass 'api' and 'unsafe' as arguments to change default behaviour
    sandbox = True
    community_id = 'clex-data'
    community = False
    url = 'https://sandbox.zenodo.org/api/deposit/depositions/'
    # get list of arguments,
    # first should be list of files to upload,
    # check if 'api and 'unsafe' are in list and
    # assign the last value as record_id, if more than one the first will be sleected
    filelist = sys.argv[1]
    inputs = sys.argv[2:]
    if len(inputs) >= 1:
        if 'community' in sys.argv[1:]:
            community = True
            inputs.remove('community')
        if 'api' in sys.argv[1:] :
            sandbox = False
            url = 'https://zenodo.org/api/deposit/depositions/'
            community_id = 'arc-coe-clex-data'
            inputs.remove('api')

    record_id = inputs[0]
    # get either sandbox or api token to connect
    token = get_token(sandbox)

    # delete selected drafts if list of id was passed or delete all drafts

    # get bucket_url for record 
    bucket_url = get_bucket(url, token, record_id)

    #read file paths from file
    with open(filelist) as f:
        file_paths = f.readlines()

    # upload all files to record, one by one
    for f in file_paths:
        print(f)
        f = f.replace('\n','')
        status = upload_file(bucket_url, token, record_id, f)


if __name__ == "__main__":
    main()
