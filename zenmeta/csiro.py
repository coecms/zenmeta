import json
from bs4 import BeautifulSoup
import sys
from util import read_json, convert_for
import json
from exception import ZenException


def process_urls(landing, related):
    """
    """
    links = []
    links.append({'DAP': landing['href']})
    for l in related:
        links.append({l['type']: l['address']})
    links_unique = [dict(t) for t in {tuple(d.items()) for d in links}]
    return links_unique


def process_description(description, data, for_codes):
    """Put together description adding to the abstract other fields that cannot be directly mapped to other fields
    """
    # retrieve all the relevant fields
    extra = ['lineage', 'credit', 'size', 'keywords']
    for k in extra:
        description += f"<p>{k.capitalize()}: {data.get(k,'')} </p>"
    org_levels = data['organisationalLevels']
    for k in [x for x in org_levels.keys() if x != 'irpHierarchy']:
        description += f"<p>{k.capitalize()}: {org_levels[k]} </p>"
    # temporarily add keywords and for_codes here as we're using FOR codes 2020 and geonetwork sues older version 
    description += f"<p>FOR codes: {', '.join(for_codes)} </p>"
    description += f"<p>Project: {data['project']['projectTitle']} </p>"
    return description 


def get_parties(data, keys):
    """ Find parties
    """
    people = [] 
    leader_name = data['leadResearcher']
    if 'contact' in keys:
        contact_name = data['contact']['contactName']
    else:
        contact_name = leader_name 
    for p in data['allNames']:
        person = { 'name': p['name'],
                   'role': 'author',
                   'orcid': p['orcidId'],
                   'org': (p['type'] != 'Person')} 
        if p['display'] == contact_name:
            person['affiliation'] = ('Commonwealth Scientific and '+
                                     'Industrial Research Organisation')
        if person not in people:
            people.append(person)
        if p['display'] == leader_name:
            leader = person.copy()
            leader['role'] = 'principalInvestigator'
            people.append(leader)
    org = {'name': 'Commonwealth Scientific and Industrial Research Organisation',
           'role': 'owner',
           'org': True}
    org['affiliation'] = org['name']
    people.append(org)
    for o in data['organisations']:
        if 'CSIRO' in o:
            continue
        else:
            org = {'name': o,
               'role': 'sponsor',
               'org': True}
            people.append(org)
    return people


def get_dates(data):
    """Get relevant dates
    some date elements have "date time type", in others date and time are joined
    string is split and last "bit" is the type, other bits are joined but only
    first 10 characters are kept for date 
    Return also year to sue for citation
    """

    publication_date = data['published'].split("T")[0]
    year = publication_date[0:4]
    return publication_date, year

def convert_spatial(spatial):
    """Convert lat lon max and min form degrees, min, seconds to decimal
       Save them in right order

    Parameters
    ----------
        spatial: list
        data['spatialParameters']
    Returns
    ----------
    geo : list
        [minLon, maxLon, minLat, maxLat]
    """
    geo = []
    for k in ["eastLongitude", "westLongitude", "southLatitude","northLatitude"]:
        deg, rest = spatial[k].split("°") 
        mins, rest = rest.split("′") 
        sec, rest = rest.split('″') 
        dd = float(deg) + float(mins)/60 + float(sec)/(60*60);
        if any(x in k for x in ['south', 'east']): 
            dd *= -1
        geo.append(dd)
    return geo 


def main():
    fname = sys.argv[1]
    data = read_json(fname)
    keys = [k for k in data.keys()]
    print(keys)
    csiro_id = data['dataCollectionId'] 

    # initial output dict
    # Retrieve string tags
    out = {}
    # these attributes are already named as we want them in output
    identical = ['title', 'doi', 'description']
    out['license'] = data['licence']
    if "," in data['keywords']:
        out['keywords'] = data['keywords'].split(",")
    else:
        out['keywords'] = data['keywords'].split(";")
    for k in identical:
        out[k] = data[k]
    out['alt_title'] = "" 

    #'metadata', 'data', 'serviceCount', 'supportingFiles',
    #'spatialParameters'

    out['license_link'] = data['licenceLink']['href']
    res_types = {'Data': {'id': 'dataset', 'title': "Dataset"},
                 'Software': {'id': 'software', 'title': "Software"},
                 'Service': {'id': 'service', 'title': 'Web service'}}
    out['resource_type'] = res_types[data['collectionContentType']]

    for_codes = data['fieldsOfResearch']
    out['for_codes'] = []
    for c in for_codes:
        codes20 = convert_for(c)
        out['for_codes'].extend(codes20)
 
    out['parties'] = get_parties(data, keys)

    out['publication_date'], year = get_dates(data)
    out['citation'] = " ".join(["<p>Preferred citation:</p>",
                               f"<p>{data['attributionStatement']}</p>"]) 
    extra = ['access', 'accessLevel', 'rights', 'dataRestricted']
    for k in extra:
        out['citation'] += f"<p>{k.capitalize()}: {data[k]} </p>"
    if 'contact' in keys:
        out['citation'] += f"<p>Contact: {' - '.join([v for v in data['contact'].values()])} </p>"
    # Links
    out['related_identifiers'] = process_urls(data.get('landingPage'), data.get('relatedLinks',[]))

    # Find geo spatial extent
    out['geospatial'] = convert_spatial(data['spatialParameters']) 
        
    out['time_coverage'] = [data['dataStartDate'], data['dataEndDate']] 
    # save 2008 version of for_codes in description
    out['description'] = process_description(out['description'], data, for_codes)
    out['version'] = ""
    out['location'] = ""
    out['fformat'] = ""
    out['publisher'] = 'CSIRO (Australia)'

    with open(f'csiro_{csiro_id}_meta.json', 'w') as fp:
        json.dump([out], fp)

if __name__ == "__main__":
    main()

# todo if only one organisation assume everybody is with csiro!
