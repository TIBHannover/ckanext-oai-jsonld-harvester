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
                harvest_obj.save()
                harvest_obj_ids.append(harvest_obj.id)
                log.debug("Harvest obj %s created" % harvest_obj.id)

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
            for ckan_field, oai_field in mapping.items():
                try:
                    package_dict[ckan_field] = content[oai_field][0]
                except (IndexError, KeyError):
                    continue

            # add author
            package_dict["author"] = self._extract_author(content)

            # add owner_org
            source_dataset = get_action("package_show")(
                context.copy(), {"id": harvest_object.source.id}
            )
            owner_org = source_dataset.get("owner_org")
            package_dict["owner_org"] = owner_org

            # add license
            package_dict["license_id"] = self._extract_license_id(context=context,content=content)


            # add resources
            url = self._get_possible_resource(harvest_object, content)
            package_dict["resources"] = self._extract_resources(url, content)

            # extract tags from 'type' and 'subject' field
            # everything else is added as extra field
            tags, extras, related_resources = self._extract_tags_and_extras(content)
            package_dict["tags"] = tags
            package_dict["extras"] = extras


            # create smiles code form inchi & add to extras table
            smiles,inchi_key,exact_mass,mol_formula = self._get_chemical_info(package_dict,content)
            extras.append({"key":"smiles", "value": smiles})
            extras.append({"key":"inchi_key", "value": inchi_key})
            extras.append({"key": "exactmass", "value": exact_mass})


            # groups aka projects
            groups = []

            # create group based on set
            if content["set_spec"]:
                log.debug("set_spec: %s" % content["set_spec"])
                groups.extend(
                    {"id": group_id}
                    for group_id in self._find_or_create_groups(
                        content["set_spec"], context.copy()
                    )
                )

            # add groups from content
            groups.extend(
                {"id": group_id}
                for group_id in self._extract_groups(content, context.copy())
            )

            package_dict["groups"] = groups

            # allow sub-classes to add additional fields
            package_dict = self._extract_additional_fields(
                content, package_dict
            )

            log.debug("Create/update package using dict: %s" % package_dict)
            self._create_or_update_package(
                package_dict, harvest_object, "package_show"
            )
            rebuild(package_dict["name"])

            Session.commit()

            log.debug("Finished record")
            log.debug(self._save_relationships_to_db(package_dict, content, smiles,inchi_key,exact_mass,mol_formula))

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
            "title": "title",
            "notes": "description",
            "maintainer": "publisher",
            "maintainer_email": "maintainer_email",
            "url": "source",
        }

    def _extract_author(self, content):
        return ", ".join(content["creator"])

    def _extract_license_id(self, context,content):
        package_license = None
        content_license = ", ".join(content["rights"])
        license_list = get_action('license_list')(context.copy(),{})
        for license_name in license_list:

            if content_license == license_name['id'] or content_license ==license_name['url'] or content_license == license_name['title']:
                package_license = license_name['id']

        return package_license


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

    def _get_possible_resource(self, harvest_obj, content):
        url = None
        candidates = content["identifier"]
        candidates.append(harvest_obj.guid)
        for ident in candidates:
            if ident.startswith("http://") or ident.startswith("https://"):
                url = ident
            elif ident.startswith("10."):
                url = "https://doi.org/" + ident
                break
        return url

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

    def _get_chemical_info(self, package ,content):

        """ function to convert InChI code to smiles code.
        This uses rdkit library to  convert available InChI to SMILES. (Chemoinformatic library)

        Database Table has been generated for molecule data.
        We use psycopg2 to connect to database and INSERT data using SQL query

        """
        #global values
        smiles = None
        inchi_key = None
        exact_mass = None
        mol_formula = None
        standard_inchi = content["inchi"]
        package_id = package['id']

        for inchi_code in standard_inchi:
            if inchi_code.startswith('InChI'):
                molecu = inchi.MolFromInchi(inchi_code)
                smiles = rdmolfiles.MolToSmiles(molecu)
                inchi_key = inchi.InchiToInchiKey(inchi_code)
                exact_mass = Descriptors.MolWt(molecu)
                mol_formula = rdMolDescriptors.CalcMolFormula(molecu)

                # upload images to folder
                try:
                    filepath = '/var/lib/ckan/default/storage/images/' + str(inchi_key) + '.png'
                    open(filepath,'w')
                    Draw.MolToFile(molecu, filepath)
                    log.debug("Molecule Image generated for %s", package_id)
                except Exception as e:
                    log.error(e)

        log.debug("Moleculer Data loaded for %s", package['id'])
        log.debug(f"Molecular Formula {mol_formula}")

        return smiles, inchi_key, exact_mass, mol_formula



    def _save_relationships_to_db(self, package, content, smiles, inchi_key, exact_mass,mol_formula):

        """ Database Table have been generated for storing related resources
        We connect to database and send those values directly  from harvested metadata
        Molecule data is also sent from this function, storing into molecule_data table"""

        package_id =    package['id']
        relation_id =   content['relation']
        relationType =  content['relationType']
        relationIdType = content['relationIdType']
        standard_inchi = content["inchi"]

        value = list(self.yield_func(package_id, relation_id,relationType,relationIdType))


        #connect to db
        con = psycopg2.connect(user = DB_USER,
                                     host =  DB_HOST,
                                     password = DB_pwd,
                                     dbname = DB_NAME )

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Cursor
        cur = con.cursor()

        # Check if the row already exists, if not then INSERT
        for val in  value:
            cur.execute(
                "SELECT * FROM related_resources WHERE package_id = %s AND relation_id = %s;", (val[0],val[1],))

            if cur.fetchone() is None:
                cur.execute("INSERT INTO related_resources VALUES (nextval('related_resources_id_seq'),%s,%s,%s,%s)", val)


        # Sending molecular information to database table(molecule_data table)

        mol_values = [package_id, json.dumps(standard_inchi), smiles, inchi_key, exact_mass,mol_formula]

        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # cursor for execution
        cur2 = con.cursor()

        # Check if the row already exists, if no then INSERT new row
        cur2.execute("SELECT * FROM molecule_data WHERE package_id = %s", (package_id,))
        if cur2.fetchone() is None:
            cur2.execute("INSERT INTO molecule_data VALUES (nextval('molecule_data_id_seq'),%s,%s,%s,%s,%s,%s)", mol_values)
        else:
            cur2.execute("INSERT INTO molecule_data(package_id,inchi,smiles,inchi_key,exact_mass,mol_formula) VALUES (%s,%s,%s,%s,%s,%s)",
                         mol_values)
        # commit cursor
        con.commit()

        # close cursor
        cur.close()
        # close cursor
        cur2.close()

        # close connection
        con.close()

        return "Data loaded to database"

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
