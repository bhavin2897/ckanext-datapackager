import random
import cgi
import json
import tempfile
import io
import six
import os.path
import time
import traceback

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

# Adding for CIF File integration
import uuid
from ase.io import read as ase_read
from ase.visualize.plot import plot_atoms
import matplotlib.pyplot as plt

log = logging.getLogger(__name__)


# DB_HOST = "localhost"
# DB_USER = "ckan_default"
# DB_NAME = "ckan_default"
# DB_pwd = "123456789"


def package_create_from_datapackage_or_cif(context, data_dict):
    upload = data_dict.get('upload')

    if upload and _is_cif_file(upload.filename):
        log.debug("Processing CIF file upload...")
        return _process_cif_and_create_package(context, data_dict)
    else:
        return package_create_from_datapackage(context, data_dict)


def _is_cif_file(filename):
    return filename.lower().endswith('.cif')


def package_create_from_datapackage(context, data_dict):
    '''Create a new dataset (package) from a Data Package file. molecule

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
    updated_datasets = []
    url = data_dict.get('url')
    upload = data_dict.get('upload')

    res = {}
    res_to_send = []
    if not url and not _upload_attribute_is_valid(upload):
        msg = {'url': ['you must define either a url or upload attribute']}
        raise toolkit.ValidationError(msg)

    dp = _load_and_validate_datapackage(url=url, upload=upload)

    # considering each JSON file has one dataset and Chemcial Substance

    for each_dp in dp:
        send_dp_to_convert = each_dp.to_dict()
        dataset_dict = converter.package(send_dp_to_convert)
        # log.debug(f'{dataset_dict}')

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
        package_show_context = {'model': model, 'session': Session,
                                'ignore_auth': True}
        res = _package_create_with_unique_name(package_show_context, dataset_dict)

        dataset_id = res['id']

        resources_data = res['resources']

        if not resources_data:
            log.debug(f'{resource_data} is not present')
            try:
                _create_resources(dataset_id, context, resources)
            except Exception as e:
                log.error(e)
                try:
                    toolkit.get_action('package_delete')(
                        context, {'id': dataset_id})
                except Exception as e2:
                    six.raise_from(e, e2)

        res['state'] = 'active'

        _send_to_db(package=res)
        _import_molecule_images(package=res)

        log.debug(f'dataset {res["id"]} will be updated')
        res_final = remove_extras_if_duplicates_exist(res)

        # log.debug(f'The final Res for dataset {res["id"]}: {res_final}')
        res_to_send.append(res_final)
        # log.debug(f'list of ress: {res_to_send}').

    package_show_context = {'model': model, 'session': Session,
                            'ignore_auth': True}
    iteration_count = 0  # Initialize counter
    for dataset in res_to_send:
        iteration_count += 1
        try:
            # Update the dataset
            updated_dataset = toolkit.get_action('package_update')(package_show_context, dataset)
            log.debug(f"Updated dataset: {updated_dataset['id']}")
            updated_datasets.append(updated_dataset)
        except toolkit.ValidationError as e:
            log.debug(f"Error updating dataset {dataset['id']}: {e.error_dict}")
        except Exception as e:
            log.debug(f"Unhandled error for dataset {dataset['id']}: {e}")
    log.debug(f'Number of dataset updated {iteration_count}')
    return updated_datasets


def _process_cif_and_create_package(context, data_dict):
    updated_datasets = []
    upload = data_dict.get('upload')
    if not upload:
        raise toolkit.ValidationError({'upload': ['No CIF file provided']})

    try:
        byte_data = upload.read()
        cif_str = byte_data.decode('utf-8')
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.cif', delete=False) as tmp_cif:
            tmp_cif.write(cif_str)
            tmp_cif_path = tmp_cif.name

        # Initialize metadata
        identifier = "N/A"
        citation_title = "N/A"
        mol_formula = "N/A"
        molecule_name = "N/A"
        authors = []
        extras = {}

        is_author_section = False

        with open(tmp_cif_path, 'r') as f:
            lines = f.readlines()
            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # Identifier
                if line.startswith("data_"):
                    identifier = line

                # Citation title - multi-line block
                elif "_citation_title" in line:
                    i += 1
                    citation_title_lines = []
                    while i < len(lines) and not lines[i].strip().startswith(";"):
                        i += 1  # skip until start of block
                    i += 1  # skip the first ;
                    while i < len(lines) and not lines[i].strip().startswith(";"):
                        citation_title_lines.append(lines[i].strip())
                        i += 1
                    citation_title = " ".join(citation_title_lines).strip()

                # Formula structural
                elif "_chemical_formula_structural" in line:
                    parts = line.split("'")
                    if len(parts) > 1:
                        mol_formula = parts[1].strip()
                    else:
                        mol_formula = line.split()[-1]

                # Chemical name common
                elif "_chemical_name_common" in line:
                    parts = line.split("'")
                    if len(parts) > 1:
                        molecule_name = parts[1].strip()
                    else:
                        molecule_name = line.split()[-1]

                # Authors
                elif "_citation_author_name" in line:
                    is_author_section = True
                elif is_author_section and line.startswith("primary"):
                    parts = line.split("'")
                    if len(parts) > 1:
                        authors.append(parts[1].strip())
                elif is_author_section and not line.startswith("primary"):
                    is_author_section = False

                # Space group extras
                elif "_space_group_name_H-M_alt" in line:
                    parts = line.split("'")
                    if len(parts) > 1:
                        extras["space_group_name_H-M"] = parts[1].strip()
                    else:
                        extras["space_group_name_H-M"] = line.split()[-1]
                elif "_space_group_IT_number" in line:
                    extras["space_group_IT_number"] = line.split()[-1]

                i += 1

        # Format authors for CKAN field
        author_str = '; '.join(authors) if authors else 'N/A'

        # Build dataset_dict
        dataset_dict = {
            'identifier': identifier,
            'name': identifier.lower().replace(' ', '_'),
            'title': citation_title or 'N/A',
            'notes': f'Molecule name: {molecule_name}',
            'mol_formula': mol_formula or 'N/A',
            'smiles': 'N/A',
            'inchi': 'N/A',
            'inchi_key': '',
            'exactmass': 'N/A',  # Will compute below
            'author': author_str,
            'license_id': data_dict.get('license_id', 'N/A'),
            'owner_org': data_dict.get('owner_org', 'N/A'),
            'language': 'english',
            'state': 'draft',
            'extras': [{'key': k, 'value': v} for k, v in extras.items()]
        }

        # Compute exactmass
        from ase.io import read as ase_read
        from ase.visualize.plot import plot_atoms
        import matplotlib.pyplot as plt

        atoms = ase_read(tmp_cif_path)
        exact_mass = atoms.get_masses().sum() if atoms else 'N/A'
        dataset_dict['exactmass'] = str(exact_mass) if exact_mass else 'N/A'

        # Save 3D PNG image (300x300 px, 300 DPI)
        img_filename = f'/var/lib/ckan/default/storage/images/{identifier}.png'
        fig, ax = plt.subplots(figsize=(1, 1), dpi=100)
        plot_atoms(atoms, ax, rotation=('45x,45y,0z'), radii=0.4, scale=0.8, show_unit_cell=0)
        plt.axis('off')
        plt.savefig(img_filename, dpi=300, transparent=True)
        plt.close()
        log.debug(f"3D CIF image saved: {img_filename}")

        # Create dataset in CKAN
        package_show_context = {'model': model, 'session': Session, 'ignore_auth': True}
        res = _package_create_with_unique_name(package_show_context, dataset_dict)

        _create_resources(dataset_id=dataset_dict['identifier'],context=context, resources=upload)
        # Remove extras duplicates
        res = remove_extras_if_duplicates_exist(res)

        res['state'] = 'active'
        _send_to_db(package=res)
        updated_dataset = toolkit.get_action('package_update')(package_show_context, res)
        updated_datasets.append(updated_dataset)

    except Exception as e:
        log.error(f'Error processing CIF file: {e}')
        raise toolkit.ValidationError({'upload': ['Failed to process CIF file']})

    finally:
        if tmp_cif_path and os.path.exists(tmp_cif_path):
            os.remove(tmp_cif_path)

    return updated_datasets

def _package_create_with_unique_name(context, dataset_dict):
    dataset_dict['name'] = dataset_dict['identifier'].lower()
    dataset_dict['id'] = munge_title_to_name(dataset_dict['name'])

    package_show_context = {'model': model, 'session': Session,
                            'ignore_auth': True}

    existing_package_dict = _find_existing_package(dataset_dict, package_show_context)

    if existing_package_dict:
        return _handle_existing_package(context, dataset_dict)
    else:
        return _create_new_package(package_show_context, dataset_dict)


def _handle_existing_package(context, dataset_dict):
    log.debug(f'Handle existing package {dataset_dict}')
    resError = None
    try:
        log.info(f'Package with GUID {dataset_dict["id"]} exists and is skipped')
        res = toolkit.get_action('package_show')(context, {'id': dataset_dict['id']})
        log.debug(f'Package with GUID {res}')

        if not res['license_title']:
            log.debug(f'Updating license...')
            res['license_id'] = _extract_license_id(context, dataset_dict)
            log.debug("Updated license")
        else:
            return res
        try:
            if not res['mol_formula']:
                log.debug(f'Updating mol formula...')
                res['mol_formula'] = dataset_dict['mol_formula']
                log.debug(f'added Molecular formula ')

            elif res['license_id'] and res['mol_formula']:
                resError = res
                log.debug(f'Nothing to update.Both Licenses and Molecular formula exits')
            else:
                return res

        except Exception as e:
            log.error(f"line 172 {e}")
            pass

        return remove_extras_if_duplicates_exist(res)
    except toolkit.ValidationError as e:
        log.error(f'Validation error at package Create #line177 {e}')
        return resError  # Or a more appropriate error handling


def _create_new_package(context, dataset_dict):
    log.debug(f'Create a new package')
    context.pop('__auth_audit', None)
    try:
        log.debug('NEW package is being created')
        res = toolkit.get_action('package_create')(context, dataset_dict)

        if dataset_dict.get('license'):
            res['license_id'] = _extract_license_id(context, dataset_dict)
        # log.debug(f"Result created {res}")
        return remove_extras_if_duplicates_exist(res)

    except toolkit.ValidationError as e:
        log.error(f'Exception during package creation #line 194: {e}')
        return _handle_package_creation_exception(context, dataset_dict, e)


def _handle_package_creation_exception(context, dataset_dict, e):
    log.debug(f'Handle package creation exception')

    if 'That URL is already in use.' in e.error_dict.get('name', []):
        dataset_dict['name'] = _generate_random_name(dataset_dict)
    elif 'Dataset id already exists' in e.error_dict.get('id', []):
        dataset_dict['id'] = _generate_random_id(dataset_dict)

    try:
        res = toolkit.get_action('package_create')(context, dataset_dict)
        if dataset_dict.get('license'):
            res['license_id'] = _extract_license_id(context, dataset_dict)
        return remove_extras_if_duplicates_exist(res)
    except toolkit.ValidationError as e:
        log.error(f'Failed to create package with exception #line 212: {e}')
        return 0  # Or a more appropriate error handling


def _generate_random_name(dataset_dict):
    random_num = random.randint(0, 9999999999)
    return f"{dataset_dict.get('name', 'dp')}-{random_num}"


def _generate_random_id(dataset_dict):
    random_num = random.randint(0, 9999999999)
    return f"{dataset_dict.get('name', 'dp')}-{random_num}"


def _load_and_validate_datapackage(url=None, upload=None):
    dp_list = []
    try:
        if _upload_attribute_is_valid(upload):
            # You will get bytes values here. Convert them to decided values.

            byte_data = upload.read()
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
                log.error(f'Invalid JSON file # line 245')

        else:
            dp = datapackage.DataPackage(url)

    except (datapackage.exceptions.DataPackageException,
            datapackage.exceptions.SchemaError,
            datapackage.exceptions.ValidationError) as e:

        msg = {'datapackage': e}
        # pass
        raise toolkit.ValidationError(msg)

    # if not dp.safe():
    #    msg = {'datapackage': ['the Data Package has unsafe attributes']}
    #    raise toolkit.ValidationError(msg)

    return dp_list


def remove_extras_if_duplicates_exist(dataset_dict):
    try:
        if 'extras' in dataset_dict is not None:
            extras_keys = [extra['key'] for extra in dataset_dict['extras']]
            main_keys = set(dataset_dict.keys()) - {'extras'}

            # Check for any duplicates
            if any(key in main_keys for key in extras_keys):
                # If duplicates found, empty 'extras'
                dataset_dict['extras'] = []
        else:
            log.debug('Nothing exists as duplicate')
    except Exception as e:
        log.error(f'remove_extras_if_duplicates_exist: {e}')
        pass
    return dataset_dict


def _create_resources(dataset_id, context, resources):
    for resource in resources:
        resource['package_id'] = dataset_id
        if resource.get('data'):
            log.debug(f'Creates Resources through inlines')
            _create_and_upload_resource_with_inline_data(context, resource)
        elif resource.get('path'):
            log.debug(f'uploading Resource locally')
            _create_and_upload_local_resource(context, resource)

        else:
            # TODO: Investigate why in test_controller the resource['url'] is a list
            if type(resource['url']) is list:
                resource['url'] = resource['url'][0]
                log.debug("RESOURCING")

                try:
                    toolkit.get_action('resource_create')(context, resource)
                    log.debug("Create a new resource")
                except Exception as e:
                    # if 'There is a schema field with the same name' in e.error_dict.get('extras', []):
                    if e is True:
                        toolkit.get_action('resource_update')(context, resource)
                    else:
                        pass
            # log.debug('Resource created')


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
    try:
        content_license = content['license']
        license_list = toolkit.get_action('license_list')(context.copy(), {})

        for license_name in license_list:
            if content_license == license_name['id'] or content_license == license_name['url'] or content_license == \
                    license_name['title']:
                package_license = license_name['id']
    except Exception as e:
        log.error(f'Error extracting license: {e}')
        pass

    return package_license


def _find_existing_package(package_dict, context):
    """
    Check if a package exists with same ID
    """
    data_dict = {'id': package_dict['id']}
    try:

        return toolkit.get_action('package_show')(context, data_dict)
    except Exception as e:
            log.error(f' find existing package error {e}')
            return 0


def _send_to_db(package):
    """
    sends the molecule information and all other informtion to database directly.
    """
    name_list = []
    package_id = package['id']
    log.debug(package)

    try:
        standard_inchi = package['inchi']
        inchi_key = package['inchi_key']
        smiles = package['smiles']
        exact_mass = package['exactmass']
        mol_formula = package['mol_formula']

        # Check if the row already exists, if not then INSERT
        molecule_id = molecules._get_inchi_from_db(inchi_key)
        log.debug(f"Current molecule_d  {molecule_id}")
        relation_value = mol_rel_data.get_mol_formula_by_package_id(package_id)
        log.debug(f"Here is the relation {relation_value}")

        # TODO: Check if relationship exists or not.

        if not molecule_id:  # if there is no molecule at all, it inserts rows into molecules and molecule_rel_data dt
            molecules.create(standard_inchi, smiles, inchi_key, exact_mass, mol_formula)
            new_molecules_id = molecules._get_inchi_from_db(inchi_key)
            new_molecules_id = new_molecules_id[0]
            # Check if relaionship exists
            log.debug(f"New molecule {new_molecules_id}")
            mol_rel_data.create(new_molecules_id, package_id)
            log.debug('data sent to molecules and relation db')

        elif not relation_value:  # if the molecule exists, but the relation doesn't exist, it create the relation
            # with molecule ID
            log.debug("Relationship must be created")
            mol_rel_data.create(molecule_id[0], package_id)
            log.debug('data sent to mol_relation db')
        else:  # if the both exists
            log.debug('Nothing to insert. Already existing')

    except Exception as e:
        if e:
            log.error(f'Sent to db not possible because of this error {e}')
            pass
        else:
            pass
    return 0


def _import_molecule_images(package):
    package_id = package['id']
    try:
        if package['inchi'] and package['inchi_key']:
            standard_inchi = package['inchi']
            inchi_key = package['inchi_key']

            if standard_inchi.startswith('InChI'):
                molecule = inchi.MolFromInchi(standard_inchi)
                log.debug("Molecule generated")
                try:
                    filepath = '/var/lib/ckan/default/storage/images/' + str(inchi_key) + '.png'
                    if os.path.isfile(filepath):
                        log.debug("Image Already exists")
                    else:
                        Draw.MolToFile(molecule, filepath)
                        log.debug("Molecule Image generated for %s", package_id)

                except Exception as e:
                    log.error(f"_import_molecule_images not possible: {e}")
            return 0

        elif package['inchi']:
            standard_inchi = package['inchi']
            if standard_inchi.startswith('InChI'):
                molecule = inchi.MolFromInchi(standard_inchi)
                inchi_key = inchi.InchiToInchiKey(standard_inchi)
                try:
                    filepath = '/var/lib/ckan/default/storage/images/' + str(inchi_key) + '.png'
                    if os.path.isfile(filepath):
                        log.debug("Image Already exists")
                    else:
                        Draw.MolToFile(molecule, filepath)
                        log.debug("Molecule Image generated for %s", package_id)
                except Exception as e:
                    log.error(f"_import_molecule_images not possible: {e}")

            return 0

        else:
            return 0

    except Exception as e:
        log.error(f"Exception occured # line 483{e}")
        return 0


# Used only in CKAN < 2.9
class _UploadLocalFileStorage(cgi.FieldStorage):
    def __init__(self, fp, *args, **kwargs):
        self.name = fp.name
        self.filename = fp.name
        self.file = fp
