import logging
import json
import re
from urllib.error import HTTPError
import traceback
import os
from datetime import datetime
from datetime import timedelta

from ckan.model import Session
from ckan.logic import get_action
from ckan import model


from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.lib.munge import munge_tag
from ckan.lib.munge import munge_title_to_name
from ckan.lib.search import rebuild
from ckanext.harvest.model import HarvestObject


import oaipmh.client
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry

from ckanext.massbankharvester.harvester.metadata import json_container_reader

from rdkit.Chem import inchi
from rdkit.Chem import rdmolfiles
from rdkit.Chem import Draw
from rdkit.Chem import Descriptors
from rdkit.Chem import rdMolDescriptors

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


log = logging.getLogger(__name__)

DB_HOST = "localhost"
DB_USER = "ckan_default"
DB_NAME = "ckan_default"
DB_pwd = "123456789"



class MassbankHarvester(HarvesterBase):
    """
    Massbank Harvester
    """
    # TODO: Check weather vaild or not

    def info(self):
        """
        Return information about this harvester.
        """
        return {
            "name": "Massbank Harvester",
            "title": "Massbank Harvester",
            "description": "Harvester for Massbank OAI Handler with BioSchemaOrg/JSON Container ",
        }

    def gather_stage(self, harvest_job):
        """

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        """
        log.debug("in gather stage: %s" % harvest_job.source.url)
        try:
            harvest_obj_ids = []
            registry = self._create_metadata_registry()
            self._set_config(harvest_job.source.config)
            client = oaipmh.client.Client(
                harvest_job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get,
            )

            client.identify()  # check if identify works

            for header in self._identifier_generator(client):
                harvest_obj = HarvestObject(
                    guid=header.identifier(), job=harvest_job
                )
                #TODO: drop if and  break for individual identifier.
                #if harvest_obj.guid == 'https://massbank.eu/MassBank/RecordDisplay?id=MSBNK-Fac_Eng_Univ_Tokyo-JP002512#VTSZSPVMHBJJIS-UHFFFAOYSA-N':
                if harvest_obj.guid == 'D506c':
                    harvest_obj.save()
                    harvest_obj_ids.append(harvest_obj.id)
                    log.debug("Harvest obj %s created" % harvest_obj.id)
                    break

        except (HTTPError) as e:
            log.exception(
                "Gather stage failed on %s (%s): %s, %s"
                % (harvest_job.source.url, e.fp.read(), e.reason, e.hdrs)
            )
            self._save_gather_error(
                "Could not gather anything from %s" % harvest_job.source.url,
                harvest_job,
            )
            return None
        except (Exception) as e:
            log.exception(
                "Gather stage failed on %s: %s"
                % (
                    harvest_job.source.url,
                    str(e),
                )
            )
            self._save_gather_error(
                "Could not gather anything from %s: %s / %s"
                % (harvest_job.source.url, str(e), traceback.format_exc()),
                harvest_job,
            )
            return None
        log.debug(
            "Gather stage successfully finished with %s harvest objects"
            % len(harvest_obj_ids)
        )
        return harvest_obj_ids

    def _identifier_generator(self, client):
        """
        pyoai generates the URL based on the given method parameters
        Therefore one may not use the set parameter if it is not there
        """

        if self.set_from or self.set_until:
            for header in client.listIdentifiers(metadataPrefix=self.md_format, set=self.set_spec,
                                                 from_= datetime.strptime(self.set_from, "%Y-%m-%dT%H:%M:%SZ"), until= datetime.strptime(self.set_until,"%Y-%m-%dT%H:%M:%SZ")):
                yield header

        elif self.set_spec:
            for header in client.listIdentifiers(metadataPrefix=self.md_format, set=self.set_spec,
                                                 from_= datetime.strptime(self.set_from, "%Y-%m-%dT%H:%M:%SZ"), until= datetime.strptime(self.set_until,"%Y-%m-%dT%H:%M:%SZ")):
                yield header

        else:
            for header in client.listIdentifiers(
                metadataPrefix=self.md_format
            ):
                yield header


    def _create_metadata_registry(self,):
        registry = MetadataRegistry()
        registry.registerReader("json_container", json_container_reader)
        return registry

    def _set_config(self, source_config):

        now = datetime.now()
        yesterday = now - timedelta(days=5)

        try:
            config_json = json.loads(source_config)
            log.debug("config_json: %s" % config_json)
            try:
                username = config_json["username"]
                password = config_json["password"]
                self.credentials = (username, password)
            except (IndexError, KeyError):
                self.credentials = None

            self.user = "harvest"
            self.set_spec = config_json.get("set", None)
            self.md_format = config_json.get("metadata_prefix", "oai_dc")
            self.set_from = config_json.get("from",str(yesterday.strftime("%Y-%m-%dT%H:%M:%SZ")))
            self.set_until = config_json.get("until",str(now.strftime("%Y-%m-%dT%H:%M:%SZ")))
            self.force_http_get = config_json.get("force_http_get", False)

        except ValueError:
            pass

    def fetch_stage(self, harvest_object):
        """
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        """
        log.debug("in fetch stage: %s" % harvest_object.guid)
        try:
            self._set_config(harvest_object.job.source.config)
            registry = self._create_metadata_registry()
            client = oaipmh.client.Client(
                harvest_object.job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get,
            )
            record = None
            try:
                log.debug(
                    "Load %s with metadata prefix '%s'"
                    % (harvest_object.guid, self.md_format)
                )

                self._before_record_fetch(harvest_object)

                record = client.getRecord(
                    identifier=harvest_object.guid,
                    metadataPrefix=self.md_format,
                )
                self._after_record_fetch(record)
                log.debug("record found!")
            except:
                log.exception("getRecord failed for %s" % harvest_object.guid)
                self._save_object_error(
                    "Get record failed for %s!" % harvest_object.guid,
                    harvest_object,
                )
                return False

            header, metadata, _ = record

            try:
                metadata_modified = header.datestamp().isoformat()
            except:
                metadata_modified = None

            try:
                content_dict = metadata.getMap()
                json_script = ''.join(content_dict['json_data'])
                data = re.sub(r'[\n ]+', ' ', json_script).strip()
                expected_finalValue = json.loads(data)
                content_dict["set_spec"] = header.setSpec()

                if metadata_modified:
                    content_dict["metadata_modified"] = metadata_modified
                log.debug(expected_finalValue)
                content = json.dumps(expected_finalValue)
            except:
                log.exception("Dumping the metadata failed!")
                self._save_object_error(
                    "Dumping the metadata failed!", harvest_object
                )
                return False

            harvest_object.content = content
            harvest_object.save()
        except (Exception) as e:
            log.exception(e)
            self._save_object_error(
                "Exception in fetch stage for %s: %r / %s"
                % (harvest_object.guid, e, traceback.format_exc()),
                harvest_object,
            )
            return False

        return True

    def _before_record_fetch(self, harvest_object):
        pass

    def _after_record_fetch(self, record):
        pass

    def import_stage(self, harvest_object):
        """
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g
              create a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package must be added to the HarvestObject.
              Additionally, the HarvestObject must be flagged as current.
            - creating the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        """

        log.debug("in import stage: %s" % harvest_object.guid)
        if not harvest_object:
            log.error("No harvest object received")
            self._save_object_error("No harvest object received")
            return False

        try:
            self._set_config(harvest_object.job.source.config)
            context = {
                "model": model,
                "session": Session,
                "user": self.user,
                "ignore_auth": True,
            }

            package_dict = {}
            content = json.loads(harvest_object.content)
            #log.debug(content)
            study = content[1]
            dataset = content[0]
            log.debug(study)

            package_dict["id"] = munge_title_to_name(harvest_object.guid)
            package_dict["name"] = package_dict["id"]

            mapping = self._get_mapping()
            for ckan_field, json_container_field in mapping.items():
                try:
                    package_dict[ckan_field] = study[json_container_field]
                except (IndexError, KeyError):
                    continue

                    # get id
            package_dict["id"] = munge_title_to_name(harvest_object.guid)

            package_dict['name'] = package_dict['id']
            package_dict['title'] = study['name']
                    #package_dict["title"] = content['headline']
            package_dict['url'] = study['url']

            # add author
            try:
                _study_isPart_ = study['isPartOf']
                citation = _study_isPart_['citation']

                log.debug('This would be citation of authors  %s', citation)
            #_study_citation_ = _study_author['citation']


                package_dict["author"] = self._extract_author(citation)
            except Exception as e:
                log.exception(e)

            # add owner_org

            source_dataset = get_action("package_show")(
                context.copy(), {"id": harvest_object.source.id}
            )
            owner_org = source_dataset.get("owner_org")
            package_dict["owner_org"] = owner_org



            '''_ adapted from Bioschema scrapper Harvester for updates _'''
            # TODO: Change according to required 'type'
            biochem_entity = study['about']
            hasBioChemEntityPart = biochem_entity['hasBioChemEntityPart']

        # add notes, license_id
            package_dict["resources"] = self._extract_resources(biochem_entity)

            package_dict['notes'] = study['description']
            #package_dict["license_id"] = self._extract_license_id(context=context, content=content)
            #log.debug(f'This is the license {package_dict["license_id"]}')

            log.debug(hasBioChemEntityPart[0])
            extras = self._extract_extras_image(package=package_dict, content=hasBioChemEntityPart[0])
            dates = self._extract_publish_dates(content = study)
            package_dict['extras'] = extras
            package_dict['extras'].append(dates)
            exact_mass = package_dict['extras']['exact_mass']
            log.debug(package_dict['extras'])

            tags = self._extract_tags(dataset)
            #package_dict['tags'] = tags

            # creating package
            log.debug("Create/update package using dict: %s" % package_dict)
            self._create_or_update_package(
                package_dict, harvest_object, "package_show"
            )

            rebuild(package_dict["name"])
            Session.commit()

            self._send_to_db(package=package_dict, content=hasBioChemEntityPart[0])

            log.debug("Finished record")

        except (Exception) as e:
            log.exception(e)
            self._save_object_error(
                "Exception in fetch stage for %s: %r / %s"
                % (harvest_object.guid, e, traceback.format_exc()),
                harvest_object,
            )
            return False
        return True

    def _get_mapping(self):
        return {
            "title": "name",
            "notes": "description",
            "maintainer": "publisher",
            "maintainer_email": "",
            "url": "url",
        }

    def _extract_author(self, content):
       return ", ".join(content["author"])

    # def _extract_license_id(self, context,content):
    #     package_license = None
    #     content_license = ", ".join(content["rights"])
    #     license_list = get_action('license_list')(context.copy(),{})
    #     for license_name in license_list:
    #
    #         if content_license == license_name['id'] or content_license ==license_name['url'] or content_license == license_name['title']:
    #             package_license = license_name['id']
    #
    #     return package_license

    ''' Extract resources from source URL'''

    def _extract_resources(self, content):
        resources = []
        url = content['url']
        log.debug("URL of resource: %s" % url)
        if url:
            try:
                resource_format = content["format"][0]
            except (IndexError, KeyError):
                resource_format = "HTML"
            resources.append(
                {
                    "name": content["name"],
                    "resource_type": resource_format,
                    "format": resource_format,
                    "url": url,
                }
            )
        return resources


    def _extract_tags(self,content):
        tags = []
        try:
            technique = [content[1]['measurementTechnique']]
            log.debug(f'this is technia {technique}')
            if technique:
                tags.extend(technique)

            tags = [{"name": munge_tag(tag[:100])} for tag in tags]
        except Exception as e:
            log.debug(e)

        return tags



# NFDI4Chem extensions for storing chemical data in respective tables

    def _extract_extras_image(self, package, content):
        extras = []
        package_id = package['id']

        standard_inchi = content['inChI']
        inchi_key = content['inChIKey']
        smiles = content['smiles'][2]
        mol_formula = content['molecularFormula']

        #molecu = inchi.MolFromInchi(standard_inchi)
        #exact_mass = Descriptors.MolWt(molecu)

        extras.append({"key": "inchi", 'value': standard_inchi})
        extras.append({"key": "inchi_key", 'value': inchi_key})
        extras.append({"key": "smiles", 'value': smiles})
        extras.append({"key": "mol_formula", 'value': mol_formula})


        if standard_inchi.startswith('InChI'):
            molecu = inchi.MolFromInchi(standard_inchi)
            exact_mass = Descriptors.MolWt(molecu)
            extras.append({'key': "exactmass", "value": exact_mass})
            log.debug("Molecule generated")
            try:
                filepath = '/var/lib/ckan/default/storage/images/' + str(inchi_key) + '.png'
                if os.path.isfile(filepath):
                    log.debug("Image Already exists")
                else:
                    Draw.MolToFile(molecu, filepath)
                    log.debug("Molecule Image generated for %s", package_id)

            except Exception as e:
                log.error(e)
        return extras

 # extracting date metadata as extra data.
    def _extract_publish_dates(self,content):

        extras = []
        #package_id = package['id']

        try:
            if content['datePublished']:
                published = content['datePublished']
                date_value = parse(published)
                date_without_tz = date_value.replace(tzinfo=None)
                value = date_without_tz.isoformat()
                extras.append({"key": "datePublished", "value": value})
            if content['dateCreated']:
                created = content['dateCreated']
                date_value = parse(created)
                date_without_tz = date_value.replace(tzinfo=None)
                value = date_without_tz.isoformat()
                extras.append({"key": "dateCreated", "value": value})
            if content['dateModified']:
                modified = content['dateModified']
                date_value = parse(modified)
                date_without_tz = date_value.replace(tzinfo=None)
                value = date_without_tz.isoformat()
                extras.append({"key": "dateModified", "value": value})
        except Exception as e:
            log.exception(e)
            pass

        return extras


    ''' Sending data to DB'''

    def _send_to_db(self, package, content):

        name_list = []
        package_id = package['id']
        standard_inchi = content['inChI']

        inchi_key = content['inChIKey']
        smiles = content['smiles'][2]
        #exact_mass = content['monoisotopicMolecularWeight']
        mol_formula = content['molecularFormula']

        # To harvest alternate Names and define them to list such that they can be dumped to database
        #alternatenames = content['alternateName']

        #if isinstance(alternatenames, list) is True:
        #    for p in alternatenames:
        #        name = [package_id, p]
        #        name_list.append(name)
        #else:
        #    name_list.append([package_id, alternatenames])

        # connect to db
        con = psycopg2.connect(user=DB_USER,
                               host=DB_HOST,
                               password=DB_pwd,
                               dbname=DB_NAME)

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        values = [package_id, json.dumps(standard_inchi), smiles, inchi_key, exact_mass, mol_formula]

        # Cursor
        cur = con.cursor()

        # Check if the row already exists, if not then INSERT

        cur.execute("SELECT * FROM molecule_data WHERE package_id = %s", (package_id,))
        if cur.fetchone() is None:
            cur.execute("INSERT INTO molecule_data VALUES (nextval('molecule_data_id_seq'),%s,%s,%s,%s,%s,%s)", values)

        cur2 = con.cursor()

        for name in name_list:
            cur2.execute("SELECT * FROM related_resources WHERE package_id = %s AND alternate_name = %s;", name)
            log.debug(f'db to {name}')
            if cur2.fetchone() is None:
                cur2.execute(
                    "INSERT INTO related_resources(id,package_id,alternate_name) VALUES(nextval('related_resources_id_seq'),%s,%s)",
                    name)
        # commit cursor
        con.commit()
        # close cursor
        cur.close()
        # close connection
        con.close()
        log.debug('data sent to db')
        return 0


    ''' To extract measuring techniques and converting them to CKAN Tag as facets'''
    #def _extract_measuring_tech(self,content):
    #
    #    tag_names = None
    #    package_title = str(content['title'])
    #
    #    #mass spectrometry
    #    mass_Exp = re.compile(r'Mass')
    #    mass_exp = re.compile(r'mass')
    #    hnmr_exp =  re.compile(r'1H NMR')
    #    cnmr_exp = re.compile(r'13C NMR')
    #    ir_exp = re.compile(r'IR')
    #    uv_exp = re.compile(r'UV')
    #
    #    if mass_exp.search(package_title) or mass_Exp.search(package_title):
    #        tag_names = ['mass-spectrometry']
    #        return tag_names
    #
    #    if hnmr_exp.search(package_title):
    #        tag_names = ['1H-NMR']
    #        return tag_names
    #
    #    if cnmr_exp.search(package_title):
    #        tag_names = ['13C-NMR']
    #        return tag_names
    #
    #    if ir_exp.search(package_title):
    #        tag_names = ['IR']
    #        return tag_names
    #
    #    if uv_exp.search(package_title):
    #        tag_names = ['UV']
    #        return tag_names
    #
    #    else:
    #        return None
    #    #tag_name = [{"name": munge_tag(tag[:100])} for tag in tag_names]


    #def yield_func(self,package_id, relation_id,relationType,relationIdType):
    #    # An yield function to return generator list values to make a single list of values
    #
    #    for p,q,r in zip(relation_id,relationType,relationIdType):
    #        value = (package_id, p,q,r )
    #        yield value
