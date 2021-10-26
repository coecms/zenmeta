from github import Github
import json, csv
import requests
import click
import os, sys

# define an ApiKey class, from https://github.com/elbaschid/click-example/blob/tutorial-2/cli.py
# also the cong file approach is taken from same example
class ApiKey(click.ParamType):
    name = 'api-key'

    def convert(self, value, param, ctx):
        found = re.match(r'[0-9a-f]{32}', value)
        if not found:
            self.fail(
                f'{value} is not a 32-character hexadecimal string',
                param, ctx)
        return value

@click.group()
@click.option(
    '--api-key', '-a',
    type=ApiKey(),
    help='your API key for Github' 
)
@click.option(
    '--config-file', '-c',
    type=click.Path(),
    default='~/.githubapi.cfg',
)
@click.pass_context
def gitmeta(ctx, api_key, config_file):
    ''' '''
    filename = os.path.expanduser(config_file)
    if not api_key and os.path.exists(filename):
        with open(filename) as cfg:
            api_key = cfg.read().replace("\n",'')
    ctx.obj = {
        'api_key': api_key,
        'config_file': filename,
    }


@gitmeta.command()
@click.pass_context
def config(ctx):
    """
    Store configuration values in a file, e.g. the API key for Github API.
    """
    config_file = ctx.obj['config_file']
    api_key = click.prompt(
        "Please enter your API key",
        default=ctx.obj.get('api_key', '')
    )
    with open(config_file, 'w') as cfg:
        cfg.write(api_key)


@gitmeta.command()
@click.pass_context
def update(ctx):
    """
    Update tables
    """
    crosswalk(True)
    
    
@gitmeta.command()
@click.argument('repository')
@click.pass_context
def codemeta(ctx, repository):
    ''' Create a draft for a .codemeta.json file.\n
        :argument: repository: repository name \n
                  The tool will assumed the repository is in the user account. \n
                  If a repository is in different namespace then pass both separated by a "/" \n
                  For example: coecms/clef 
    '''
    repo = get_repo(repository)
    print(repo)
    extract_info(repo, 'codemeta')
    pass
    return


def crosswalk(update):
    ''' Read crosswalk codemeta-github file 
        If 'update' True then redownload crosswalk.csv from web and save to file
    '''
    if update:
        get_file('https://raw.githubusercontent.com/codemeta/codemeta/master/crosswalk.csv','crosswalk.csv')
        read_csv('data/crosswalk.csv','data/crosswalk.json','pretty')
    else:
        pass
        #with open('data/crosswalk.csv','r') as f:
    return 
         

def get_file(url,fname):
    ''' download file from url return content'''
    response = requests.get(url)
    with open(os.path.join("data", fname), 'wb') as f:
        f.write(response.content)
    return 

def read_csv(file, json_file, format):
    csv_rows = []
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        title = reader.fieldnames
        for row in reader:
            csv_rows.extend([{title[i]:row[title[i]] for i in range(len(title))}])
        write_json(csv_rows, json_file, format)

#Convert csv data into json and write it
def write_json(data, json_file, format):
    with open(json_file, "w") as f:
        if format == "pretty":
            f.write(json.dumps(data, sort_keys=False, indent=4, 
                               separators=(',', ': '),
                               ensure_ascii=False))
            #f.write(json.dumps(data, sort_keys=False, indent=4, separators=(',', ': '),encoding="utf-8",ensure_ascii=False))
        else:
            f.write(json.dumps(data))

def get_crosswalk():
    ''' Scrape crosswalk table from codemeta website '''
    url='https://codemeta.github.io/terms/'
    r=requests.get(url)
    page_text = r.text.encode('utf-8').decode('ascii', 'ignore')
    soup=bs(page_text,'html.parser')
    skeys = soup.table.find_all('th')
    keys = [k.text.replace("\n","") for k in skeys]
    rows = soup.table.tbody.find_all('tr')
    terms={}
    for row in rows:
        cols =  row.find_all('td')
        vals = [c.text.replace("\n","") for c in cols]
        terms[vals[0]] = {keys[1]: vals[1], keys[2]: vals[2]}
    return terms

def extract_info(repo, flag):
    ''' '''
    meta={}
#print(dir(repo))
    print(repo.name)
    print(repo.full_name)
    print(repo.topics)
    topics = repo.get_topics()
    print(topics)
    print(repo.ssh_url)
    print(repo.url)
    print(repo.language)
    print(repo.homepage)
    print(repo.created_at)
    print(repo.organization)
    flicense = repo.get_license()
    freadme = repo.get_readme()
    print(flicense.decoded_content)
    print(freadme.decoded_content)
# should have .json file somewhere lode this as keys and values and create function to read them in
    codeRepository=repo.html_url
    programmingLanguage=repo.languages_url
    downloadUrl=repo.archive_url
    author=repo.login
    dateCreated=repo.created_at
    dateModified=repo.updated_at
    license=repo.license
    description=repo.description
    identifier=repo.id
    name=repo.full_name
    issueTracker=repo.issues_url
    return meta
     
    
@gitmeta.command()
@click.argument('repo')
@click.pass_context
def zenodo(ctx, repo):
    ''' create a draft for a .zenodo.json file '''
    pass
    return

@click.pass_context
def get_repo(ctx, repository):
    ''' Open a github API session using the API_key and retrieve repository object '''
    g = Github(ctx.obj['api_key'])
    user = g.get_user()
    repos = [r for r in user.get_repos()]
    # I want to double check repo name is valid, repo could be in coecms or somewhere else!!
    # maybe we should pass as an option where repo is like we assume is in whatever acocunt linked to the token, or otherwise they have to specify coecms/
    # if we find a "/" in repo name we use that as environment or even url?///// if it fails then print message
    #try:
    #    coecms = g('https://github.com/coecms')
    #    cmsrepos = [r for r in coecms.get_repos()]
    #    print(cmsrepos)
    #except:
    #    cmsrepos=[]
    #for o in :
    #    repos.extend(o.get_repos)
    if repository not in repos.extend(cmsrepos):
        print(f'Could not find a repository called {repository} for user {user}')
        print(f'Valid repository names are:')
        for r in repos:
            print(r)
        for r in cmsrepos:
            print(f'{coecms}/{r}')
        sys.exit()
    else:
        repo = g.get_repo(repository)
    return repo 


def random_col():

    # to see all the available attributes and methods
# Github Enterprise with custom hostname
#g = Github(base_url="https://{hostname}/api/v3", login_or_token="access_token")
# create the zenodo and codemeta json dictionaries
    zenodo = {}

if __name__ == "__main__":
    gitmeta()

  #  repos =  g.get_user().get_repos()
