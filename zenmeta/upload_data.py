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
        #fname = '.zenodo'
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


def post_json(url, token, data):
    """
    """
    headers = {"Content-Type": "application/json"}
    r = requests.post(url,
            params={'access_token': token}, json=data,
            headers=headers)
    print(data['metadata']['title'])
    print(r.status_code)
    if r.status_code in  [400, 403, 500]:
        print(r.text)


def process_author(author):
    """
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
    """
    ind = license.find('Attribution')
    if ind == -1:
         print('check license for this record')
         return {'id': 'cc-by-4.0'}
    return {'id': license[0:ind].strip().replace(' ','-').lower() + '-4.0'}


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

def main():

    # define urls, input file and if loading to sandbox or production
    global authors
    #authors = {}
    authors = read_json('authors.json') 
    url='https://sandbox.zenodo.org/api/deposit/depositions'
    community_id = 'clex-data'
    sandbox = True
    fname = sys.argv[1]
    if len(sys.argv) == 3:
        if sys.argv[2] == 'production':
            sandbox = False
            url='https://zenodo.org/api/deposit/depositions'
            community_id = 'arc-coe-clex-data'

    # get either sandbox or api token to connect
    token = get_token(sandbox)

    # read data from input json file and process plans in file
    data = read_json(fname)
    # process data for each plan and post records returned by process_plan()
    #for plan in data:
    #    record = process_plan(plan, community_id)
    #    post_json(url, token, record)
    post_json(url, token, data)
    #with open('latest_authors.json', 'w') as fp:
    #    json.dump(authors, fp)

if __name__ == "__main__":
    main()
