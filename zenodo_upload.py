import requests
with open('.zenodo','r') as f:
     ztoken=f.readline().replace("\n","")
with open('.sandbox','r') as f:
     stoken=f.readline().replace("\n","")
headers = {"Content-Type": "application/json"}
test_url='https://sandbox.zenodo.org/api/deposit/depositions'
api_url='https://zenodo.org/api/deposit/depositions'
data = {
    'metadata': {
        'title': 'CleF Climate Finder',
        'upload_type': 'software',
        'description': 'CleF test upload. CleF - Climate Finder - Dataset search tool developed by the CLEX CMS team, powered by ESGF and the NCI MAS database.\n Clef searches the Earth System Grid Federation datasets stored at the Australian National Computational Infrastructure, both data published on the NCI ESGF node as well as files that are locally replicated from other ESGF nodes.',
        'creators': [
             {"name": 'Wales, Scott',
              "affiliation": 'Melbourne University',
              "orcid": '0000-0002-0583-9601'},
             {"name": 'Petrelli, Paola',
              "affiliation": 'University of Tasmania',
              "orcid": '0000-0002-0164-5105'}
        ],
       'access_right': 'embargoed',
       'license': 'Apache-2.0',
       'embargo_date': '2018-11-01',
       'keywords': ['ESGF', 'NCI', 'CMIP','python'],
       #'communities': [{'identifier': 'arc-coe-clex'}],
       #'grants': [{'id':''}],
       #'subjects': [{"term": "Climate",
       #              "identifier": ""}],
       'version': '0.3.0',
       'language': 'eng'
     },
     'modified': '20180924',
     'state': 'inprogress',
     'submitted': False
}
r = requests.post(test_url,
                params={'access_token': stoken}, json=data,
                headers=headers)
print(r.status_code)
print(r.json())
