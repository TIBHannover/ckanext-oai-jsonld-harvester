[![Tests](https://github.com/bhavin2897/ckanext-oai-jsonld-harvester/workflows/Tests/badge.svg?branch=main)](https://github.com/bhavin2897/ckanext-massbankharvester/actions)

# ckanext-oai-jsonld-harvester

OAI-PMH Harvester for JSON-LD metadata on CKAN, along with Chemistry metadata harvesting.

NFDI4Chem is working on a strategy to harvest metadata using OAI protocol for JSON-LD metadata. This extension provides different harvesting procedures for different Bioschema.Org types which can be later migrated to CKAN database.
This also combines two important harvester that have been already developed for NFDI4Chem Search Service, [ckanext-oaipmh](https://github.com/TIBHannover/ckanext-oaipmh) and [ckanext-bioschemaharvester](https://github.com/TIBHannover/ckanext-bioschemaharvester). 

Just like the harvester mentioned above, this harvester also uses RDKit python module to generate graphical/imaginary representation of molecules and also other chemi-infomatics. 


## Requirements

Compatibility with core CKAN versions:

| CKAN version    | Compatible?   |
| --------------- | ------------- |
| 2.8 & earlier            | not tested   |
| 2.9             | YES    |


## Installation


To install ckanext-oai-jsonld-harvester:

1. Activate your CKAN virtual environment, for example:

     `. /usr/lib/ckan/default/bin/activate`

2. Clone the source and install it on the virtualenv 

   `git clone https://github.com/bhavin2897/ckanext-oai-jsonld-harvester.git`

       cd ckanext-oai-jsonld-harvester

       pip install -e .

       pip install -r requirements.txt 

Note: This extension works on RDKit chemi-informatics library which is used to generated molecular information and 
molecular images during harvesting. And also migration tables are necessary for further database storage. 

4. Add `massbankharvester` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

5. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     `sudo service apache2 reload`
   (and) 

   While using production server, reload your servers. 
   
   `sudo service supervisor reload && sudo service nginx reload `

## OAI-PMH JSON-LD Harvesting & Configuration Set-Up.
Firstly, log-in as `harvest` user. 
* with the web browser go to `<your ckan url>/harvest/new`

* Fill the source URL from which you like to obtain datasets from (an OAI-PMH JSON-LD repository) 

During harvesting, it is important to follow OAI-PMh vocabulary to obtain desired datasets.

* if your OAI-PMH needs credentials, add the following to the "Configuration" section: {"username": "foo", "password": "bar" }

* if you only want to harvest a specific set, add the following to the "Configuration" section: {"set": "baz"}

* To harvest JSON-LD data in BioSchema format, add the following to the "Configuration" section: {"metadata_prefix": "json_container"} (also supports `oai-dc`)

* Please add the `setSpec` from which the source metadata to be harvested from to the "Configuration" section: {"set":"MassBank:DataSets"}

* if you want harvest during a time duration, use {"from": "2020-09-20T00:00:01Z" & "until": "2021-01-01T00:00:01Z"} Please follow OAI-PMH guides line for using timestamps http://www.openarchives.org/OAI/openarchivesprotocol.html#DatestampsRequests

* if your OAI-PMH source does not support HTTP POST and you want to enforce HTTP GET, add the following to the "Configuration" section: {"force_http_get": true} (defaults to false)

* Save

* on the harvest admin click `Reharvest`

#### Configuration Example

   ``` {
      "metadata_prefix": "json_container",
      "set":"MassBank:DataSets",
      "from": "2022-12-01T00:00:01Z",
      "force_http_get": true
      } 
   ```

**Note**: Please check the behaviour of the source URL, for which sets and duration of time have to be used before using the harvester. 
As it would be difficult for debugging each datasets for a huge repository.

## Run the Harvester

On the command line do this:

    activate the python environment
    cd to the ckan directory, e.g. /usr/lib/ckan/default/src/ckan
    start the consumers:

ckan -c /etc/ckan/default/ckan.ini harvester gather_consumer &
ckan -c /etc/ckan/default/ckan.ini harvester fetch_consumer &

    run the job:

    ckan -c /etc/ckan/default/ckan.ini harvester run

The harvester should now start and import the OAI-PMH metadata.

## Developer installation

To install ckanext-oai-jsonld-harvester for development, activate your CKAN virtualenv and
do:

      git clone https://github.com/bhavin2897/ckanext-oai-jsonld-harvester.git
      cd ckanext-oai-jsonld-harvester
      python setup.py develop
      pip install -r dev-requirements.txt `


## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini

To do unit testing:
* mention the GUID for one specific identifier on the commondline and force harvest a particular dataset. 

   ``` harvester run {source-id/name} force-import=guid1... ``` 

## Releasing a new version of ckanext-oai-jsonld-harvester

If ckanext-oai-jsonld-harvester should be available on PyPI you can follow these steps to publish a new version:

1. Update the version number in the `setup.py` file. See [PEP 440](http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers) for how to choose version numbers.

2. Make sure you have the latest version of necessary packages:

    pip install --upgrade setuptools wheel twine

3. Create a source and binary distributions of the new version:

       python setup.py sdist bdist_wheel && twine check dist/*

   Fix any errors you get.

4. Upload the source distribution to PyPI:

       twine upload dist/*

5. Commit any outstanding changes:

       git commit -a
       git push

6. Tag the new release of the project on GitHub with the version number from
   the `setup.py` file. For example if the version number in `setup.py` is
   0.0.1 then do:

       git tag 0.0.1
       git push --tags

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
