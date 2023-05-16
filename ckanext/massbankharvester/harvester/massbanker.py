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
                #TODO: drop if and  break
                if harvest_obj.guid == 'https://massbank.eu/MassBank/RecordDisplay?id=MSBNK-Fac_Eng_Univ_Tokyo-JP002512#VTSZSPVMHBJJIS-UHFFFAOYSA-N':
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
            log.debug(content)

            package_dict["id"] = munge_title_to_name(harvest_object.guid)
            package_dict["name"] = package_dict["id"]

            mapping = self._get_mapping()
            for ckan_field, json_container_field in mapping.items():
                try:
                    package_dict[ckan_field] = content[json_container_field][0]
                except (IndexError, KeyError):
                    continue

                    # get id
            package_dict["id"] = munge_title_to_name(harvest_object.guid)

            package_dict['name'] = package_dict['id']
            package_dict['title'] = content['name']
                    #package_dict["title"] = content['headline']
            package_dict['url'] = content['url']

            # add author
            #package_dict["author"] = self._extract_author(content)

            # add owner_org
            #source_dataset = get_action("package_show")(
             #   context.copy(), {"id": harvest_object.source.id}
            #)
            #owner_org = source_dataset.get("owner_org")
            #package_dict["owner_org"] = owner_org

            # add license
            #package_dict["license_id"] = self._extract_license_id(context=context,content=content)


            # add resources
            #url = self._get_possible_resource(harvest_object, content)

            #url = content['url']

           # package_dict["resources"] = self._extract_resources(url, content)
           #
           # # extract tags from 'type' and 'subject' field
           # # everything else is added as extra field
           # tags, extras, related_resources = self._extract_tags_and_extras(content)
           # package_dict["tags"] = tags
           # package_dict["extras"] = extras
           #
           #
           # # create smiles code form inchi & add to extras table
           # smiles,inchi_key,exact_mass,mol_formula = self._get_chemical_info(package_dict,content)
           # extras.append({"key":"smiles", "value": smiles})
           # extras.append({"key":"inchi_key", "value": inchi_key})
           # extras.append({"key": "exactmass", "value": exact_mass})
           #
           #
           # # groups aka projects
           # groups = []
           #
           # # create group based on set
           # if content["set_spec"]:
           #     log.debug("set_spec: %s" % content["set_spec"])
           #     groups.extend(
           #         {"id": group_id}
           #         for group_id in self._find_or_create_groups(
           #             content["set_spec"], context.copy()
           #         )
           #     )
           #
           # # add groups from content
           # groups.extend(
           #     {"id": group_id}
           #     for group_id in self._extract_groups(content, context.copy())
           # )
           #
           # package_dict["groups"] = groups
           #
           # # allow sub-classes to add additional fields
           # package_dict = self._extract_additional_fields(
           #     content, package_dict
           # )
           #
           # log.debug("Create/update package using dict: %s" % package_dict)
           # self._create_or_update_package(
           #     package_dict, harvest_object, "package_show"
           # )
           # rebuild(package_dict["name"])
           #
           # Session.commit()
           #
           # log.debug("Finished record")
           # log.debug(self._save_relationships_to_db(package_dict, content, smiles,inchi_key,exact_mass,mol_formula))




            # add notes, license_id
            package_dict['notes'] = content['description']
            #package_dict["license_id"] = self._extract_license_id(context=context, content=content)
            #log.debug(f'This is the license {package_dict["license_id"]}')

            extras = self._extract_extras_image(package=package_dict, content=content)
            package_dict['extras'] = extras

            tags = self._extract_tags(content)
            package_dict['tags'] = tags

            # creating package
            log.debug("Create/update package using dict: %s" % package_dict)
            self._create_or_update_package(
                package_dict, harvest_object, "package_show"
            )

            rebuild(package_dict["name"])
            Session.commit()

            self._send_to_db(package=package_dict, content=content)

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

    #def _extract_author(self, content):
    #    return ", ".join(content["creator"])

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


    def _extract_tags_and_extras(self, content):
        extras = []
        tags = []
        related_resources = []

        for key, value in content.items():
            if key in self._get_mapping().values():
                continue
            if key in ["type", "subject"]:
                if type(value) is list:
                    tags.extend(value)
                else:
                    tags.extend(value.split(";"))
                continue
            if value and type(value) is list:
                    # To harvest related and relationType without raising any exceptions
                    if key == 'relation' or key == 'relationType':
                        try:
                            value = value
                        except Exception:
                            pass
                    else:
                        value = value[0]
            if not value:
                value = None
            if key.endswith("date") and value:
                # the ckan indexer can't handle timezone-aware datetime objects
                try:
                    from dateutil.parser import parse
                    date_value = parse(value)
                    date_without_tz = date_value.replace(tzinfo=None)
                    value = date_without_tz.isoformat()
                except (ValueError, TypeError):
                    continue
            extras.append({"key": key, "value": value})

        tag_tech = self._extract_measuring_tech(content)
        if tag_tech:
            tags.extend(tag_tech)
        tags = [{"name": munge_tag(tag[:100])} for tag in tags]

        return (tags, extras, related_resources)


    def _extract_resources(self, url, content):
        resources = []
        log.debug("URL of resource: %s" % url)
        if url:
            try:
                resource_format = content["format"][0]
            except (IndexError, KeyError):
                resource_format = "HTML"
            resources.append(
                {
                    "name": content["title"][0],
                    "resource_type": resource_format,
                    "format": resource_format,
                    "url": url,
                }
            )
        return resources

    def _extract_groups(self, content, context):
        if "series" in content and len(content["series"]) > 0:
            return self._find_or_create_groups(content["series"], context)
        return []

    def _extract_additional_fields(self, content, package_dict):
        # This method is the ideal place for sub-classes to
        # change whatever they want in the package_dict
        return package_dict

    def _find_or_create_groups(self, groups, context):
        log.debug("Group names: %s" % groups)
        group_ids = []
        for group_name in groups:
            data_dict = {
                "id": group_name,
                "name": munge_title_to_name(group_name),
                "title": group_name,
            }
            try:
                group = get_action("group_show")(context.copy(), data_dict)
                log.info("found the group " + group["id"])
            except:
                group = get_action("group_create")(context.copy(), data_dict)
                log.info("created the group " + group["id"])
            group_ids.append(group["id"])

        log.debug("Group ids: %s" % group_ids)
        return group_ids


# NFDI4Chem extensions for storing chemical data in respective tables

    def _extract_extras_image(self, package, content):
        extras = []
        package_id = package['id']

        standard_inchi = content['inChI']

        inchi_key = content['inchikey']
        smiles = content['smiles']
        exact_mass = content['monoisotopicMolecularWeight']

        extras.append({"key": "inchi", 'value': standard_inchi})
        extras.append({"key": "inchi_key", 'value': inchi_key})
        extras.append({"key": "smiles", 'value': smiles})
        extras.append({'key': "exactmass", "value": exact_mass})

        if standard_inchi.startswith('InChI'):
            molecu = inchi.MolFromInchi(standard_inchi)
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

                # extracting date metadata as extra data.
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

        inchi_key = content['inchikey']
        smiles = content['smiles']
        exact_mass = content['monoisotopicMolecularWeight']
        mol_formula = content['molecularFormula']

        # To harvest alternate Names and define them to list such that they can be dumped to database
        alternatenames = content['alternateName']

        if isinstance(alternatenames, list) is True:
            for p in alternatenames:
                name = [package_id, p]
                name_list.append(name)
        else:
            name_list.append([package_id, alternatenames])

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

    def _extract_measuring_tech(self,content):

        tag_names = None
        package_title = str(content['title'])

        #mass spectrometry
        mass_Exp = re.compile(r'Mass')
        mass_exp = re.compile(r'mass')
        hnmr_exp =  re.compile(r'1H NMR')
        cnmr_exp = re.compile(r'13C NMR')
        ir_exp = re.compile(r'IR')
        uv_exp = re.compile(r'UV')

        if mass_exp.search(package_title) or mass_Exp.search(package_title):
            tag_names = ['mass-spectrometry']
            return tag_names

        if hnmr_exp.search(package_title):
            tag_names = ['1H-NMR']
            return tag_names

        if cnmr_exp.search(package_title):
            tag_names = ['13C-NMR']
            return tag_names

        if ir_exp.search(package_title):
            tag_names = ['IR']
            return tag_names

        if uv_exp.search(package_title):
            tag_names = ['UV']
            return tag_names

        else:
            return None
        #tag_name = [{"name": munge_tag(tag[:100])} for tag in tag_names]


    def yield_func(self,package_id, relation_id,relationType,relationIdType):
        # An yield function to return generator list values to make a single list of values

        for p,q,r in zip(relation_id,relationType,relationIdType):
            value = (package_id, p,q,r )
            yield value
