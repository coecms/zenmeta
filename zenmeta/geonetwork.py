import json
from bs4 import BeautifulSoup
import sys

# Finding all instances of tag
def find_string(soup, tag, multiple=False):
    """Find tag of a string element if not None return text
    """
    val = ""
    field  = soup.find(tag)
    if field is not None:
        if multiple:
            val = field.text.split()
        else:
            val = field.text.strip()
    return val



def main():
    fname = sys.argv[1]
    geo_id = fname.split(".")[0]
    with open(fname, 'r') as f:
        data = f.read()
    # parse xml with Beautiful Soup
    soup = BeautifulSoup(data, "xml")

    # initial output dict
    # Retrieve string tags
    out = {}
    out['title'] = find_string(soup, 'title')
    out['alt_title'] = find_string(soup, 'alternateTitle')
    abstract = find_string(soup, 'abstract')
    out['doi'] = find_string(soup, 'dataSetURI')
    out['license'] = find_string(soup, 'useLimitation')
    path = find_string(soup, 'mediumName')
    update = find_string(soup, 'maintenanceAndUpdateFrequency')
    fformat = find_string(soup, 'MD_Format')
    lineage = find_string(soup, 'LI_Lineage')
    credit = find_string(soup, 'credit')
    classification = find_string(soup, 'MD_ClassificationCode')
    project = find_string(soup, 'code')

    # Retrieve for codes and keywords
    code = find_string(soup, 'abs_code')
    term = find_string(soup, 'abs_code_description')
    for_codes = {}
    if code != "":
        for_codes[code] = term
    keywords = []
    keys = soup.find_all('keyword')
    for k in keys:
        key = k.text.strip()
        if key[0] == "0":
            bits = key.split()
            if bits[0] not in for_codes.keys():
                for_codes[bits[0]] = " ".join(bits[1:]) 
        else:
            keywords.append(key)
    out['keywords'] = keywords
    out['for_codes'] = for_codes
 
    # Find parties
    parties = soup.find_all('CI_ResponsibleParty')
    people = [] 
    for p in parties:
        role = p.find('CI_RoleCode').text.strip()
        if role in ['author', 'owner', 'funder', 'principalInvestigator']:
            person = { 'name': p.find('individualName').text.strip(),
                       'affiliation' : p.find('organisationName').text.strip(),
                       'role': role, 'org': False} 
            if person['name'] == person['affiliation']:
                person['org'] = True
            people.append(person)
    out['contributors'] = people

    # Links
    urls = soup.find_all('URL')#.text.strip().split()
    out['links'] = [x.text for x in urls]
    # Find geo spatial extent
    out['geospatial'] = find_string(soup, 'EX_GeographicBoundingBox', multiple=True)
    out['time_period'] = find_string(soup, 'TimePeriod', multiple=True)
    # relevant dates
    # some date elements have "date time type", in others date and time are joined
    # string is split and last "bit" is the type, other bits are joined but only
    # first 10 characters are kept for date 
    dates = {}
    ci_dates = soup.find_all('CI_Date')
    for d in ci_dates:
        bits = d.text.split()
        dtype = bits[-1]
        date = " ".join(bits[:-1])[0:10] 
        dates[dtype] = date 
    out['dates'] = dates
    out['location'] = 'Direct access to the data is available on the NCI servers:\n' \
           + f'project: https://my.nci.org.au/mancini/login?next=/mancini/project/{project}\n' \
           + f'path: {path}'
    if dates['publication'] != "":
        year = dates['publication'][0:4]
    elif dates['creation'] != "":
        year = dates['creation'][0:4]
    else:
        year = 'YYYY'
    out['citation'] = "Preferred citation:\n" + f"<authors> ({year}): {out['title']} " \
               f"NCI Australia. (Dataset). https://dx.doi.org/{out['doi']} \n" + \
               "If accessing from NCI thredds you can also ackwonledge the service:\n" + \
               "NCI Australia (2021): NCI THREDDS Data Service. NCI Australia. (Service) https://dx.doi.org/10.25914/608bfc062f4c7"

    official_record = "Official metadata and access to the data is via the NCI geonetwork record in related identifiers."
    out['description'] = "\n\n".join( [abstract, official_record, lineage, credit, fformat, classification, update] ) 

    with open(f'{geo_id}.json', 'w') as fp:
        json.dump(out, fp)

if __name__ == "__main__":
    main()
