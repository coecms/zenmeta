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

def process_urls(urls, geo_id):
    """
    """
    links = []
    for x in urls:
        url = x.text
        if 'researchdata.edu.au' in url:
            links.append({'RDA': url})
        elif 'thredds' in url:
            links.append({'TDS': url})
        else:
            links.append({'other': url})
    links.append({'geonetwork': f"https://geonetwork.nci.org.au/geonetwork/srv/eng/catalog.search#/metadata/{geo_id}"})
    return links

def process_description(soup, keywords, for_codes):
    """Put together description adding to the abstract other fields that cannot be directly mapped to other fields
    """

    # retireieve all the relevant fields
    abstract = find_string(soup, 'abstract')
    update = "<p>Update: " + find_string(soup, 'maintenanceAndUpdateFrequency') + "</p>"
    fformat = "<p>Format: " + find_string(soup, 'MD_Format') + "</p>"
    lineage = "<p>Lineage: " + find_string(soup, 'LI_Lineage') + "</p>"
    credit = "<p>Credit: " + find_string(soup, 'credit') + "</p>"
    classification = "<p>Classification: " + find_string(soup, 'MD_ClassificationCode') + "</p>"
    official_record = "<p>Official metadata and access to the data is via the NCI geonetwork record in related identifiers.</p>"
    # temporarily add keywords and for_codes here as we're using FOR codes 2020 and geonetwork sues older version 
    keystr = f"<p>Keywords: {', '.join(keywords)}</p>"
    codestr = f"<p>FOR codes: {' '.join([f'{str(k)} - {str(v)}' for k,v in for_codes.items()])}</p>"
    description = "".join( [abstract, official_record, lineage, credit, fformat,
                                         classification, update, keystr, codestr] ) 
    return description


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
    out['doi'] = find_string(soup, 'dataSetURI')
    out['license'] = find_string(soup, 'useLimitation')
    path = find_string(soup, 'mediumName')
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
        #role = p.find('CI_RoleCode').text.strip()
        role = find_string(p,'CI_RoleCode')
        if role in ['author', 'owner', 'funder', 'principalInvestigator']:
            person = { 'name': find_string(p, 'individualName'),
                       'affiliation' : find_string(p, 'organisationName'),
                       'role': role, 'org': False} 
            #person = { 'name': p.find('individualName').text.strip(),
            #           'affiliation' : p.find('organisationName').text.strip(),
            #           'role': role, 'org': False} 
            if person['name'] == person['affiliation']:
                person['org'] = True
            people.append(person)
    out['contributors'] = people

    # Links
    urls = soup.find_all('URL')#.text.strip().split()
    out['links'] = process_urls(urls, geo_id)
    # Find geo spatial extent
    out['geospatial'] = find_string(soup, 'EX_GeographicBoundingBox', multiple=True)
    out['time_coverage'] = find_string(soup, 'TimePeriod', multiple=True)
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
    out['location'] = "".join(["<p>Direct access to the data is available on the NCI servers:</p>",
           f"<p>project: https://my.nci.org.au/mancini/login?next=/mancini/project/{project}</p>",
           f"<p>path: {path}</p>"])
    if dates['publication'] != "":
        year = dates['publication'][0:4]
    elif dates['creation'] != "":
        year = dates['creation'][0:4]
    else:
        year = 'YYYY'
    out['description'] = process_description(soup, keywords, for_codes)
    out['citation'] = " ".join(["<p>Preferred citation:</p>", f"<p><authors> ({year}): {out['title']}",
               f"NCI Australia. (Dataset). https://dx.doi.org/{out['doi']}</p>",
               "<p>If accessing from NCI thredds you can also ackwnoledge the service:</p>",
               "<p>NCI Australia (2021): NCI THREDDS Data Service. NCI Australia. (Service)",
               "https://dx.doi.org/10.25914/608bfc062f4c7</p>"])


    with open(f'{geo_id}.json', 'w') as fp:
        json.dump([out], fp)

if __name__ == "__main__":
    main()
