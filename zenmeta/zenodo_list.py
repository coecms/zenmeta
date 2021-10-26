import requests
with open('/home/581/pxp581/.zenodo','r') as f:
     ztoken=f.readline().replace("\n","")
#with open('/home/581/pxp581/.sandbox','r') as f:
#     stoken=f.readline().replace("\n","")
#print(ztoken)
#headers = {"Content-Type": "application/json"}
#headers = {"Content-Type": "application/x-bibtex"}
headers = {"Content-Type": "text/x-bibliography"}
test_url='https://sandbox.zenodo.org/api/deposit/depositions'
api_url='https://zenodo.org/api/deposit/depositions'
#api_url='https://zenodo.org/communities/arc-coe-clex/'
r = requests.get(api_url,
        params={'access_token': ztoken, 'status': 'published',
                 'communities': 'arc-coe-clex'},
                headers=headers)
#print(r.json())
print(r.text)
