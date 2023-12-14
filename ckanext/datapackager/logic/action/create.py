import random
import cgi
import json
import tempfile
import io
import six
import os.path

import ckan.plugins.toolkit as toolkit
from ckanext.datapackager.logic.action import schema4chem_ckan_mapper as converter
from werkzeug.datastructures import FileStorage

from ckanext.rdkit_visuals.models.molecule_tab import Molecules as molecules
from ckanext.rdkit_visuals.models.molecule_rel import MolecularRelationData as mol_rel_data

from rdkit.Chem import inchi
from rdkit.Chem import rdmolfiles
from rdkit.Chem import Draw
from rdkit.Chem import Descriptors

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from ckan import model
from ckan.model import Session, Package, PACKAGE_NAME_MAX_LENGTH

from ckan.lib.munge import munge_title_to_name

import logging

import datapackage

log = logging.getLogger(__name__)


DB_HOST = "localhost"
DB_USER = "ckan_default"
DB_NAME = "ckan_default"
DB_pwd = "123456789"



def package_create_from_datapackage(context, data_dict):
    '''Create a new dataset (package) from a Data Package file.

    :param url: url of the datapackage (optional if `upload` is defined)
    :type url: string
    :param upload: the uploaded datapackage (optional if `url` is defined)
    :type upload: cgi.FieldStorage
    :param name: the name of the new dataset, must be between 2 and 100
        characters long and contain only lowercase alphanumeric characters,
        ``-`` and ``_``, e.g. ``'warandpeace'`` (optional, default:
        datapackage's name concatenated with a random string to avoid
        name collisions)
    :type name: string
    :param private: the visibility of the new dataset
    :type private: bool
    :param owner_org: the id of the dataset's owning organization, see
        :py:func:`~ckan.logic.action.get.organization_list` or
        :py:func:`~ckan.logic.action.get.organization_list_for_user` for
        available values (optional)
   :type owner_org: string
    '''
    url = data_dict.get('url')
    upload = data_dict.get('upload')

    res = {}
    res_to_send = []
    if not url and not _upload_attribute_is_valid(upload):
        msg = {'url': ['you must define either a url or upload attribute']}
        raise toolkit.ValidationError(msg)


    dp = _load_and_validate_datapackage(url=url, upload=upload)

    # considering each JSON file has one dataset and ChemcialSubstance

    for each_dp in dp:
        send_dp_to_convert = each_dp.to_dict()
        dataset_dict = converter.package(send_dp_to_convert)
        #log.debug(f'{dataset_dict}')

        owner_org = data_dict.get('owner_org')

        if owner_org:
            dataset_dict['owner_org'] = owner_org

        private = data_dict.get('private')

        if private:
            dataset_dict['private'] = toolkit.asbool(private)

        name = dataset_dict['identifier']
        name = name.lower()

        resources = dataset_dict.get('resources', [])

        # Create as draft by default so if there's any issue on creating the
        # resources and we're unable to purge the dataset, at least it's not shown.
        dataset_dict['state'] = 'draft'

        res = _package_create_with_unique_name(context, dataset_dict)

        dataset_id = res['id']

        _create_resources(dataset_id, context, resources)
        #resources_to_display = res['resources']

        #if resources:
        #    package_show_context = {'model': model, 'session': Session,
        #                            'ignore_auth': True}
        #    try:
        #        _create_resources(dataset_id, context, resources)
        #        res = toolkit.get_action('package_show')(
        #            package_show_context, {'id': dataset_id})
        #
        #    except Exception as e:
        #        log.error(e)
        #        try:
        #            toolkit.get_action('package_delete')(
        #                context, {'id': dataset_id})
        #        except Exception as e2:
        #            six.raise_from(e, e2)
        #        else:
        #            raise e

        res['state'] = 'active'

        _send_to_db(package=res)
        _import_molecule_images(package=res)

        log.debug(f'dataset {res["id"]} will be updated')
        res_final = remove_extras_if_duplicates_exist(res)

        log.debug(f'The final Res for dataset {res["id"]}: {res_final}')
        res_to_send.append(res_final)
        #log.debug(f'list of ress: {res_to_send}')

    for dataset in res_to_send:

        updated_datasets = []
        try:
            # Update the dataset
            updated_dataset = toolkit.get_action('package_update')(context, dataset)
            print(f"Updated dataset: {updated_dataset['id']}")
            updated_datasets.append(updated_dataset)
        except toolkit.ValidationError as e:
            print(f"Error updating dataset {dataset['id']}: {e.error_dict}")
        except Exception as e:
            print(f"Unhandled error for dataset {dataset['id']}: {e}")

    return updated_datasets




def _load_and_validate_datapackage(url=None, upload=None):
    dp_list = []
    try:
        if _upload_attribute_is_valid(upload):
            # You will get bytes values here. Convert them to decided values.

            byte_data = upload.getvalue()
            decoded_upload = byte_data.decode('utf-8')
            try:
                json_data_upload_list = json.loads(decoded_upload)
                if toolkit.check_ckan_version(min_version="2.9"):
                    for json_data_upload in json_data_upload_list:
                        # Converted JSON to CKAN Dict
                        dp = datapackage.DataPackage(json_data_upload)
                        dp_list.append(dp)
                else:
                    dp = datapackage.DataPackage(upload.file)

            except json.JSONDecodeError:
                log.error(f'Invalid JSON file')

        else:
            dp = datapackage.DataPackage(url)

    except (datapackage.exceptions.DataPackageException,
            datapackage.exceptions.SchemaError,
            datapackage.exceptions.ValidationError) as e:

        msg = {'datapackage': e}
        # pass
        raise toolkit.ValidationError(msg)

    #if not dp.safe():
    #    msg = {'datapackage': ['the Data Package has unsafe attributes']}
    #    raise toolkit.ValidationError(msg)

    return dp_list


def _package_create_with_unique_name(context, dataset_dict):
    res = None
    package_dict_form = 'package_show'
    package_show_context = {'model': model, 'session': Session,
                            'ignore_auth': True}

    dataset_dict['name'] = dataset_dict['identifier'].lower()
    dataset_dict['id'] = munge_title_to_name(dataset_dict['name'])

    existing_package_dict = _find_existing_package(dataset_dict,context)

    if existing_package_dict:
        try:
            log.info('Package with GUID %s exists and is skipped' % dataset_dict['id'])
            res = toolkit.get_action('package_show')(context, {'id': dataset_dict['id']})

        except toolkit.ValidationError as e:
            log.error(e)
            if 'There is a schema field with the same name' in e.error_dict.get('extras', []):
                res = toolkit.get_action('package_show')(context, {'id': dataset_dict['id']})
            else:
                res = toolkit.get_action('package_show')(context, {'id': dataset_dict['id']})

    else:
        try:
            log.debug(f'NEW package is being created')
            res = toolkit.get_action('package_create')(package_show_context, dataset_dict)

            if dataset_dict['license']:
                res['license_id'] = _extract_license_id(context, dataset_dict)

        except toolkit.ValidationError as e:
            #log.debug(e)
            log.debug(f'NEW package is being created with an exception')
            if 'That URL is already in use.' in e.error_dict.get('name', []):
                random_num = random.randint(0, 9999999999)
                name = '{name}-{rand}'.format(name=dataset_dict.get('name', 'dp'),
                                              rand=random_num)
                dataset_dict['name'] = name
                res = toolkit.get_action('package_create')(package_show_context, dataset_dict)
                try:
                    if dataset_dict['license']:
                        res['license_id'] = _extract_license_id(context, dataset_dict)
                except KeyError as e:
                    log.error(e)


    #log.debug(f'res_final from package_create {res}')
    res_final = remove_extras_if_duplicates_exist(res)
    #log.debug(f'{res}')
    return res_final


def remove_extras_if_duplicates_exist(dataset_dict):
    if 'extras' in dataset_dict:
        extras_keys = [extra['key'] for extra in dataset_dict['extras']]
        main_keys = set(dataset_dict.keys()) - {'extras'}

        # Check for any duplicates
        if any(key in main_keys for key in extras_keys):
            # If duplicates found, empty 'extras'
            dataset_dict['extras'] = []
    else:
        log.debug('Nothing')
    return dataset_dict


def _create_resources(dataset_id, context, resources):
    for resource in resources:
        resource['package_id'] = dataset_id
        if resource.get('data'):
            _create_and_upload_resource_with_inline_data(context, resource)
        elif resource.get('path'):
            _create_and_upload_local_resource(context, resource)
        else:
            # TODO: Investigate why in test_controller the resource['url'] is a list
            if type(resource['url']) is list:
                resource['url'] = resource['url'][0]
            #log.debug("RESOURCING")
            try:
                toolkit.get_action('resource_create')(context, resource)
            except Exception as e:
                #if 'There is a schema field with the same name' in e.error_dict.get('extras', []):
                if e is True:
                    toolkit.get_action('resource_update')(context, resource)
                else:
                    pass
            #log.debug('Resource created')

def _create_and_upload_resource_with_inline_data(context, resource):
    prefix = resource.get('name', 'tmp')
    data = resource['data']

    del resource['data']
    if not isinstance(data, six.string_types):
        data = json.dumps(data, indent=2)

    with tempfile.NamedTemporaryFile(prefix=prefix) as f:
        if six.PY3:
            f.write(six.binary_type(data, 'utf-8'))
        else:
            f.write(six.binary_type(data))
        f.seek(0)

        _create_and_upload_resource(context, resource, f)


def _create_and_upload_local_resource(context, resource):
    path = resource['path']
    del resource['path']
    if isinstance(path, list):
        path = path[0]
    try:
        with open(path, 'r') as f:
            _create_and_upload_resource(context, resource, f)
    except IOError:
        msg = {'datapackage': [(
            "Couldn't create some of the resources."
            " Please make sure that all resources' files are accessible."
        )]}
        raise toolkit.ValidationError(msg)


def _create_and_upload_resource(context, resource, the_file):
    resource['url'] = 'url'
    resource['url_type'] = 'upload'

    if toolkit.check_ckan_version(min_version="2.9"):
        resource['upload'] = FileStorage(the_file, the_file.name, the_file.name)
    else:
        resource['upload'] = _UploadLocalFileStorage(the_file)

    toolkit.get_action('resource_create')(context, resource)


def _upload_attribute_is_valid(upload):
    return hasattr(upload, 'read') or hasattr(upload, 'file') and hasattr(upload.file, 'read')


def _extract_license_id(context, content):
    package_license = None
    content_license = content['license']
    license_list = toolkit.get_action('license_list')(context.copy(), {})
    for license_name in license_list:

        if content_license == license_name['id'] or content_license == license_name['url'] or content_license == \
                license_name['title']:
            package_license = license_name['id']

    return package_license


def _find_existing_package(package_dict,context):
    """
    Check if a package exists with same ID
    """
    data_dict = {'id': package_dict['id']}
    try:

        return toolkit.get_action('package_show')(context, data_dict)
    except Exception as e:
        if e:
            return 0

def _send_to_db(package):

    """
    sends the molecule information and all other informtion to database directly.
    """
    name_list = []
    package_id = package['id']
    #log.debug(package)

    standard_inchi = package['inchi']

    inchi_key = package['inchi_key']
    smiles = package['smiles']
    exact_mass = package['exactmass']
    mol_formula = package['mol_formula']


    # Cursor and conect to DB
    # connect to db
    con = psycopg2.connect(user=DB_USER,
                           host=DB_HOST,
                           password=DB_pwd,
                           dbname=DB_NAME)

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    # cur = con.cursor()
    cur2 = con.cursor()

    # Check if the row already exists, if not then INSERT
    molecule_id = molecules._get_inchi_from_db(inchi_key)

    if molecule_id is None:
        molecules.create(standard_inchi, smiles, inchi_key, exact_mass, mol_formula)
        new_molecules_id = molecules._get_inchi_from_db(inchi_key)
        new_molecules_id = new_molecules_id[0]
        cur2.execute("INSERT INTO molecule_rel_data (molecules_id, package_id) VALUES (%s, %s)",
                     (new_molecules_id, package_id))
        log.debug('data sent to db')
    else:
        log.debug('Nothing to insert. Already existing')
    # cur3 = con.cursor()
    #
    # for name in name_list:
    #    cur3.execute("SELECT * FROM related_resources WHERE package_id = %s AND alternate_name = %s;", name)
    #    #log.debug(f'db to {name}')
    #    if cur3.fetchone() is None:
    #        cur3.execute("INSERT INTO related_resources(id,package_id,alternate_name) VALUES(nextval('related_resources_id_seq'),%s,%s)", name)
    #
    ## commit cursor
    # con.commit()
    ## close cursor
    # cur.close()
    ## close connection
    # con.close()
    # log.debug('data sent to db')
    return 0


def _import_molecule_images(package):

    package_id = package['id']
    standard_inchi = package['inchi']
    inchi_key = package['inchi_key']

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

    return 0


# Used only in CKAN < 2.9
class _UploadLocalFileStorage(cgi.FieldStorage):
    def __init__(self, fp, *args, **kwargs):
        self.name = fp.name
        self.filename = fp.name
        self.file = fp
