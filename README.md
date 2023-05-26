[![Tests](https://github.com/bhavin2897/ckanext-oai-jsonld-harvester/workflows/Tests/badge.svg?branch=main)](https://github.com/bhavin2897/ckanext-massbankharvester/actions)

# ckanext-oai-jsonld-harvester

OAI-PMH Harvester for JSON-LD metadata on CKAN, along with Chemistry metadata harvesting.

NFDI4Chem is working on a stratedy to harvest metadata usin OAI protocol for JSON-LD metadata. This extension provides different harvesting procedures for different Bioschema.Org types which can be later migrated to CKAN database. 

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

       pip install -r requirements.txt `

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



## Config settings

None at present

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
