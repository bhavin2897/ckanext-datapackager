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
        log.debug("Processing txt file upload...")
        return _process_txt_and_create_package(context, data_dict)
    else:
        return package_create_from_datapackage(context, data_dict)


def _is_cif_file(filename):
    return filename.lower().endswith('.txt')


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

    res_to_send = []
    if not url and not _upload_attribute_is_valid(upload):
        msg = {'url': ['you must define either a url or upload attribute']}
        raise toolkit.ValidationError(msg)

    datapackages = _load_and_validate_datapackage(url=url, upload=upload)

    # Prepare and validate the complete upload before making any CKAN changes.
    prepared_datasets = _prepare_schemaorg_datasets(
        [item.to_dict() for item in datapackages]
    )
    converted_datasets = []
    for item in prepared_datasets:
        dataset_dict = converter.package(item)
        dataset_dict['_molecule_names'] = item.get('_molecule_names', [])
        converted_datasets.append(dataset_dict)

    for dataset_dict in converted_datasets:
        molecule_names = dataset_dict.pop('_molecule_names', [])

        owner_org = data_dict.get('owner_org')

        if owner_org:
            dataset_dict['owner_org'] = owner_org

        private = data_dict.get('private')

        if private:
            dataset_dict['private'] = toolkit.asbool(private)

        resources = dataset_dict.get('resources', [])

        # Create as draft by default so if there's any issue on creating the
        # resources and we're unable to purge the dataset, at least it's not shown.
        dataset_dict['state'] = 'draft'
        package_show_context = {'model': model, 'session': Session,
                                'ignore_auth': True}
        package_id = munge_title_to_name(dataset_dict['identifier'].lower())
        package_existed = bool(_find_existing_package(
            {'id': package_id}, package_show_context))
        res = _package_create_with_unique_name(package_show_context, dataset_dict)

        dataset_id = res['id']

        resources_data = res.get('resources', [])

        if not resources_data:
            try:
                _create_resources(dataset_id, context, resources)
            except Exception as e:
                log.exception("Resource import failed for Dataset %s", dataset_dict['identifier'])
                if not package_existed:
                    try:
                        toolkit.get_action('package_delete')(
                            context, {'id': dataset_id})
                    except Exception as delete_error:
                        six.raise_from(e, delete_error)
                raise toolkit.ValidationError({
                    'resources': ['Resource import failed for Dataset {}'.format(
                        dataset_dict['identifier'])]
                })

        res['state'] = 'active'

        _synchronize_related_molecule(res, molecule_names)
        _import_molecule_images(package=res)

        log.debug(f'dataset {res["id"]} will be updated')
        res_final = remove_extras_if_duplicates_exist(res)

        res_to_send.append(res_final)

    package_show_context = {'model': model, 'session': Session,
                            'ignore_auth': True}
    iteration_count = 0  # Initialize counter
    for dataset in res_to_send:
        iteration_count += 1
        updated_dataset = toolkit.get_action('package_update')(
            package_show_context, dataset)
        log.debug("Updated dataset: %s", updated_dataset['id'])
        updated_datasets.append(updated_dataset)
    log.debug(f'Number of dataset updated {iteration_count}')
    return updated_datasets


def _schemaorg_type(item):
    object_type = item.get('@type', 'Dataset')
    if isinstance(object_type, list):
        object_type = object_type[0] if object_type else 'Dataset'
    return str(object_type).rsplit('/', 1)[-1].rsplit('#', 1)[-1]


def _prepare_schemaorg_datasets(objects):
    """Pair Schema.org objects and return Dataset dicts ready for mapping."""
    grouped = {}
    order = []

    for item in objects:
        if not isinstance(item, dict):
            log.error("Invalid Schema.org object type %s", type(item).__name__)
            raise toolkit.ValidationError({
                'datapackage': ['Every top-level item must be a JSON object']
            })

        object_type = _schemaorg_type(item)
        identifier = item.get('identifier') or (
            item.get('name') if object_type == 'Dataset' else None)
        if not identifier:
            log.error("Missing identifier for Schema.org %s object", object_type)
            raise toolkit.ValidationError({
                'identifier': ['{} object is missing an identifier'.format(object_type)]
            })

        normalized_identifier = six.text_type(identifier).lower()
        if normalized_identifier not in grouped:
            grouped[normalized_identifier] = {
                'identifier': identifier, 'datasets': [], 'substances': []}
            order.append(normalized_identifier)

        if object_type == 'Dataset':
            grouped[normalized_identifier]['datasets'].append(item)
        elif object_type == 'ChemicalSubstance':
            grouped[normalized_identifier]['substances'].append(item)
        elif object_type == 'MolecularEntity':
            log.error("Top-level MolecularEntity is not supported for identifier %s", identifier)
            raise toolkit.ValidationError({
                'datapackage': [
                    'MolecularEntity {} must be nested in hasBioChemEntityPart'.format(identifier)]
            })
        else:
            log.error("Unsupported Schema.org object type %s for identifier %s",
                      object_type, identifier)
            raise toolkit.ValidationError({
                'datapackage': ['Unsupported object type {} for identifier {}'.format(
                    object_type, identifier)]
            })

    prepared = []
    chemical_mapping = {
        'inChI': 'inChI',
        'inChIKey': 'inChIKey',
        'smiles': 'smiles',
        'molecularFormula': 'molecularFormula',
        'monoisotopicMolecularWeight': 'monoisotopicMolecularWeight',
    }
    for normalized_identifier in order:
        group = grouped[normalized_identifier]
        identifier = group['identifier']
        if len(group['datasets']) != 1:
            log.error("Expected exactly one Dataset for identifier %s; found %d",
                      identifier, len(group['datasets']))
            raise toolkit.ValidationError({
                'datapackage': [
                    'Identifier {} must have exactly one Dataset object; found {}'.format(
                        identifier, len(group['datasets']))]
            })
        if len(group['substances']) > 1:
            log.error("Expected at most one ChemicalSubstance for identifier %s; found %d",
                      identifier, len(group['substances']))
            raise toolkit.ValidationError({
                'datapackage': [
                    'Identifier {} has more than one ChemicalSubstance object'.format(identifier)]
            })

        dataset = dict(group['datasets'][0])
        if group['substances']:
            substance = group['substances'][0]
            entity_parts = substance.get('hasBioChemEntityPart') or []
            if isinstance(entity_parts, dict):
                entity_parts = [entity_parts]
            if not isinstance(entity_parts, list):
                log.error("Invalid hasBioChemEntityPart for identifier %s", identifier)
                raise toolkit.ValidationError({
                    'hasBioChemEntityPart': [
                        'Expected an object or list for identifier {}'.format(identifier)]
                })

            molecular_entities = [
                part for part in entity_parts
                if isinstance(part, dict) and _schemaorg_type(part) == 'MolecularEntity'
            ]
            if len(molecular_entities) > 1:
                log.error("Multiple MolecularEntity objects for identifier %s", identifier)
                raise toolkit.ValidationError({
                    'hasBioChemEntityPart': [
                        'Identifier {} has more than one MolecularEntity'.format(identifier)]
                })
            if molecular_entities:
                entity = molecular_entities[0]
                for source_key, target_key in chemical_mapping.items():
                    if entity.get(source_key) is not None:
                        dataset[target_key] = entity.get(source_key)
            if not dataset.get('molecularFormula') and substance.get('chemicalComposition'):
                dataset['chemicalComposition'] = substance.get('chemicalComposition')
            molecule_names = [substance.get('name')]
            alternate_names = substance.get('alternateName') or []
            if not isinstance(alternate_names, list):
                alternate_names = [alternate_names]
            molecule_names.extend(alternate_names)
            dataset['_molecule_names'] = [
                name for name in molecule_names
                if isinstance(name, six.string_types) and name.strip()
            ]

        prepared.append(dataset)

    return prepared


# def _process_cif_and_create_package(context, data_dict):
#     updated_datasets = []
#     upload = data_dict.get('upload')
#     if not upload:
#         raise toolkit.ValidationError({'upload': ['No CIF file provided']})
#
#     try:
#         byte_data = upload.read()
#         cif_str = byte_data.decode('utf-8')
#         with tempfile.NamedTemporaryFile(mode='w+', suffix='.cif', delete=False) as tmp_cif:
#             tmp_cif.write(cif_str)
#             tmp_cif_path = tmp_cif.name
#
#         # Initialize metadata
#         identifier = "N/A"
#         citation_title = "N/A"
#         mol_formula = "N/A"
#         molecule_name = "N/A"
#         authors = []
#         extras = {}
#
#         is_author_section = False
#
#         with open(tmp_cif_path, 'r') as f:
#             lines = f.readlines()
#             i = 0
#             while i < len(lines):
#                 line = lines[i].strip()
#
#                 # Identifier
#                 if line.startswith("data_"):
#                     identifier = line
#
#                 # Citation title - multi-line block
#                 elif "_citation_title" in line:
#                     i += 1
#                     citation_title_lines = []
#                     while i < len(lines) and not lines[i].strip().startswith(";"):
#                         i += 1  # skip until start of block
#                     i += 1  # skip the first ;
#                     while i < len(lines) and not lines[i].strip().startswith(";"):
#                         citation_title_lines.append(lines[i].strip())
#                         i += 1
#                     citation_title = " ".join(citation_title_lines).strip()
#
#                 # Formula structural
#                 elif "_chemical_formula_structural" in line:
#                     parts = line.split("'")
#                     if len(parts) > 1:
#                         mol_formula = parts[1].strip()
#                     else:
#                         mol_formula = line.split()[-1]
#
#                 # Chemical name common
#                 elif "_chemical_name_common" in line:
#                     parts = line.split("'")
#                     if len(parts) > 1:
#                         molecule_name = parts[1].strip()
#                     else:
#                         molecule_name = line.split()[-1]
#
#                 # Authors
#                 elif "_citation_author_name" in line:
#                     is_author_section = True
#                 elif is_author_section and line.startswith("primary"):
#                     parts = line.split("'")
#                     if len(parts) > 1:
#                         authors.append(parts[1].strip())
#                 elif is_author_section and not line.startswith("primary"):
#                     is_author_section = False
#
#                 # Space group extras
#                 elif "_space_group_name_H-M_alt" in line:
#                     parts = line.split("'")
#                     if len(parts) > 1:
#                         extras["space_group_name_H-M"] = parts[1].strip()
#                     else:
#                         extras["space_group_name_H-M"] = line.split()[-1]
#                 elif "_space_group_IT_number" in line:
#                     extras["space_group_IT_number"] = line.split()[-1]
#
#                 i += 1
#
#         # Format authors for CKAN field
#         author_str = '; '.join(authors) if authors else 'N/A'
#
#         # Build dataset_dict
#         dataset_dict = {
#             'identifier': identifier,
#             'name': identifier.lower().replace(' ', '_'),
#             'title': molecule_name or 'N/A',
#             'notes': f'Crystallographic dataset of {molecule_name} which is {citation_title}',
#             'mol_formula': mol_formula or 'N/A',
#             'smiles': 'N/A',
#             'inchi': 'N/A',
#             'inchi_key': '',
#             'exactmass': 'N/A',  # Will compute below
#             'author': author_str,
#             'license_id': data_dict.get('license_id', 'N/A'),
#             'license_url': data_dict.get('license_url', 'N/A'),
#             'owner_org': data_dict.get('owner_org', 'N/A'),
#             'language': 'english',
#             'state': 'draft',
#             'extras': [{'key': k, 'value': v} for k, v in extras.items()]
#             #'extras' : []
#         }
#
#         # Compute exactmass
#         from ase.io import read as ase_read
#         from ase.visualize.plot import plot_atoms
#         import matplotlib.pyplot as plt
#
#         atoms = ase_read(tmp_cif_path)
#         exact_mass = atoms.get_masses().sum() if atoms else 'N/A'
#         dataset_dict['exactmass'] = str(exact_mass) if exact_mass else 'N/A'
#
#         # Save 3D PNG image (300x300 px, 300 DPI)
#         img_filename = f'/var/lib/ckan/default/storage/images/{identifier}.png'
#         fig, ax = plt.subplots(figsize=(1, 1), dpi=100)
#         plot_atoms(atoms, ax, rotation=('45x,45y,0z'), radii=0.4, scale=0.8, show_unit_cell=0)
#         plt.axis('off')
#         plt.savefig(img_filename, dpi=300, transparent=True)
#         plt.close()
#         log.debug(f"3D CIF image saved: {img_filename}")
#
#         # Create dataset in CKAN
#         package_show_context = {'model': model, 'session': Session, 'ignore_auth': True}
#         res = _package_create_with_unique_name(package_show_context, dataset_dict)
#
#         _create_resources(dataset_id=dataset_dict['identifier'],context=context, resources=upload)
#         # Remove extras duplicates
#         res = remove_extras_if_duplicates_exist(res)
#         log.debug('remove extras success')
#
#         res['state'] = 'active'
#         _send_to_db(package=res)
#         updated_dataset = toolkit.get_action('package_update')(package_show_context, res)
#         updated_datasets.append(updated_dataset)
#
#     except Exception as e:
#         log.error(f'Error processing CIF file why: {e}')
#         raise toolkit.ValidationError({'upload': ['Failed to process CIF file']})
#
#     finally:
#         if tmp_cif_path and os.path.exists(tmp_cif_path):
#             os.remove(tmp_cif_path)
#
#     return updated_datasets


def _process_txt_and_create_package(context, data_dict):
    updated_datasets = []
    upload = data_dict.get('upload')
    if not upload:
        raise toolkit.ValidationError({'upload': ['No TXT file provided']})

    try:
        byte_data = upload.read()
        txt_str = byte_data.decode('utf-8')
        lines = txt_str.splitlines()

        # Initialize metadata
        identifier = "N/A"
        citation_title = "N/A"
        mol_formula = "N/A"
        molecule_name = "N/A"
        authors = []
        doi = ''
        extras = {}
        reference_info = ''

        # New extras extracted from .txt file
        for line in lines:
            line = line.strip()

            if line.startswith("Coll Code"):
                identifier = f"data_{line.split()[-1]}"
            elif line.startswith("Chem Name"):
                molecule_name = line.split("Chem Name")[1].strip()
            elif line.startswith("Rec  Date "):
                metadata_published=line.split("Rec  Date ")[1].strip()
            elif line.startswith("Structured"):
                mol_formula = line.split("Structured")[1].strip()
            elif line.startswith("Title"):
                citation_title = line.split("Title")[1].strip()
            elif line.startswith("Author(s)"):
                authors_line = line.split("Author(s)")[1].strip()
                authors = [a.strip() for a in authors_line.split(";")]
            elif line.startswith("DOI"):
                doi = line.split("DOI")[1].strip()
            elif line.startswith("Space Group"):
                extras["Hermann-Mauguin space group symbol"] = line.split("Space Group")[1].strip()
            elif line.startswith("SG Number"):
                extras["Space group number"] = line.split("SG Number")[1].strip()
            elif line.startswith("Vol"):
                extras["Cell volume"] = line.split("Vol")[1].strip() + " Å³"
            elif line.startswith("Unit Cell"):
                unit_cell = line.split("Unit Cell")[1].strip().split()
                if len(unit_cell) >= 6:
                    extras["a"] = unit_cell[0] + " Å"
                    extras["b"] = unit_cell[1] + " Å"
                    extras["c"] = unit_cell[2] + " Å"
                    extras["α"] = unit_cell[3] + "°"
                    extras["β"] = unit_cell[4] + "°"
                    extras["γ"] = unit_cell[5] + "°"
            elif line.startswith("Cryst Sys"):
                extras["Crystal system"] = line.split("Cryst Sys")[1].strip()
            elif line.startswith("Pearson"):
                extras["Pearson symbol"] = line.split("Pearson")[1].strip()
            elif line.startswith("Z"):
                extras["Z"] = line.split("Z")[1].strip()
            elif line.startswith("Formula Wt"): # difference between exact mass and formula weight
                extras["Formula weight"] = line.split("Formula Wt")[1].strip()
            elif line.startswith("Sum"):
                extras["Calculated formula"] = line.split("Sum")[1].strip()

            lines = iter(lines)  # Ensure lines is an iterator before using next()
            for line in lines:
                if line.startswith("Reference"):
                    journal_lines = [line.split("Reference")[1].strip()]
                    try:
                        next_line = next(lines).strip()
                        if next_line.startswith("("):  # year, volume, pages likely here
                            journal_lines.append(next_line)
                    except StopIteration:
                        pass  # End of file reached; nothing more to append
                    reference_info = " ".join(journal_lines)

        author_str = '; '.join(authors) if authors else 'N/A'
        excluded_extras = {'license_url', 'license_id', 'author', 'identifier', 'owner_org'}

        citation = f'{author_str}, “{citation_title},” {reference_info}.'

        extras["Citation"] = citation

        dataset_dict = {
            'identifier': identifier,
            'name': identifier.lower().replace(' ', '_'),
            'doi' : doi,
            'title': molecule_name or 'N/A',
            'notes': f'Crystallographic dataset of {molecule_name} which is {citation_title}',
            'mol_formula': mol_formula or 'N/A',
            'smiles': '',
            'inchi': '',
            'inchi_key': identifier.lower().replace(' ', '_'),
            'exactmass': '',
            'author': author_str,
            'license_url':  'N/A',
            'owner_org': data_dict.get('owner_org', 'N/A'),
            'language': 'english',
            'metadata_published': metadata_published,
            'state': 'draft',
            'extras': [{'key': k, 'value': v} for k, v in extras.items() if k not in excluded_extras]

        }

        # Create dataset in CKAN
        package_show_context = {'model': model, 'session': Session, 'ignore_auth': True}
        res = _package_create_with_unique_name(package_show_context, dataset_dict)

        _create_resources(dataset_id=dataset_dict['identifier'], context=context, resources=upload)
        res = remove_extras_if_duplicates_exist(res)

        res['state'] = 'active'
        # _send_to_db(package=res)
        updated_dataset = toolkit.get_action('package_update')(package_show_context, res)
        updated_datasets.append(updated_dataset)

    except Exception as e:
        log.error(f'Error processing TXT file: {e}')
        raise toolkit.ValidationError({'upload': ['Failed to process TXT file']})

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
    log.info("Updating existing package %s", dataset_dict['id'])
    res = toolkit.get_action('package_show')(context, {'id': dataset_dict['id']})

    if not res.get('license_id') and dataset_dict.get('license'):
        license_id = _extract_license_id(context, dataset_dict)
        if license_id:
            res['license_id'] = license_id

    mapped_fields = (
        'author', 'author_email', 'maintainer', 'maintainer_email',
        'title', 'notes', 'url', 'language', 'metadata_published',
        'measurement_technique', 'measurement_technique_iri',
        'inchi', 'inchi_key', 'smiles', 'mol_formula', 'exactmass')
    for field in mapped_fields:
        incoming = dataset_dict.get(field)
        if incoming not in (None, '', [], {}) and res.get(field) != incoming:
            res[field] = incoming

    return remove_extras_if_duplicates_exist(res)


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
                if isinstance(json_data_upload_list, dict):
                    json_data_upload_list = [json_data_upload_list]
                if not isinstance(json_data_upload_list, list):
                    raise toolkit.ValidationError({
                        'datapackage': ['Uploaded JSON must be an object or an array of objects']
                    })
                if toolkit.check_ckan_version(min_version="2.9"):
                    for json_data_upload in json_data_upload_list:
                        # Converted JSON to CKAN Dict
                        dp = datapackage.DataPackage(json_data_upload)
                        dp_list.append(dp)
                else:
                    dp = datapackage.DataPackage(upload.file)

            except ValueError as e:
                log.error("Invalid uploaded JSON: %s", e)
                raise toolkit.ValidationError({'datapackage': ['Invalid JSON upload']})

        else:
            dp = datapackage.DataPackage(url)
            dp_list.append(dp)

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
        if 'extras' in dataset_dict and dataset_dict['extras'] is not None:
            extras_cleaned = []
            # CKAN schema fields to always exclude from extras
            known_fields = {
                'license_url', 'license_id', 'author', 'owner_org',
                'name', 'notes', 'title', 'language', 'state',
                'identifier', 'smiles', 'inchi', 'inchi_key',
                'mol_formula', 'exactmass', 'doi'
            }
            main_keys = set(dataset_dict.keys()).union(known_fields)

            for extra in dataset_dict['extras']:
                if extra['key'] not in main_keys:
                    extras_cleaned.append(extra)
                else:
                    log.warning(f"Removed duplicate or schema 'extras' key: {extra['key']}")

            dataset_dict['extras'] = extras_cleaned
    except Exception as e:
        log.error(f'remove_extras_if_duplicates_exist: {e}')
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
            if isinstance(resource.get('url'), list):
                resource['url'] = resource['url'][0]
            toolkit.get_action('resource_create')(context, resource)
            log.debug("Created resource %s for package %s",
                      resource.get('name'), dataset_id)


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
    content_license = content.get('license')
    if not content_license:
        return package_license
    license_list = toolkit.get_action('license_list')(context.copy(), {})

    for license_name in license_list:
        if content_license in (
                license_name.get('id'), license_name.get('url'),
                license_name.get('title')):
            package_license = license_name.get('id')
            break

    return package_license


def _find_existing_package(package_dict, context):
    """
    Check if a package exists with same ID
    """
    data_dict = {'id': package_dict['id']}
    try:
        return toolkit.get_action('package_show')(context, data_dict)
    except toolkit.ObjectNotFound:
        log.debug("No existing package found for %s", package_dict['id'])
        return None


def _synchronize_related_molecule(package, names=None, synchronizer=None):
    """Use harvester4chem's canonical molecule-package relationship service."""
    identifier = package.get('identifier') or package.get('name') or package.get('id')
    if not package.get('inchi_key'):
        log.info("Dataset %s has no InChIKey; molecule synchronization skipped",
                 identifier)
        return None

    if synchronizer is None:
        try:
            from ckanext.harvester4chem.molecule_sync import (
                MoleculeSyncError, synchronize_molecule)
        except ImportError as error:
            log.error("Molecule synchronization service is unavailable for Dataset %s",
                      identifier)
            raise toolkit.ValidationError({
                'related_molecule': ['Molecule synchronization service is unavailable']
            })
        synchronizer = synchronize_molecule
    else:
        MoleculeSyncError = RuntimeError

    try:
        result = synchronizer(
            package.get('id'), package.get('inchi'), package.get('inchi_key'),
            package.get('smiles'), package.get('mol_formula'),
            package.get('exactmass'), names=names,
            name_source='MassBank datapackage')
    except MoleculeSyncError as error:
        log.error("Molecule synchronization failed for Dataset %s: %s",
                  identifier, error)
        raise toolkit.ValidationError({
            'related_molecule': [str(error)]
        })

    log.info(
        "Dataset %s molecule=%s relationship=%s molecule_package_id=%s",
        identifier, result.get('molecule_package'),
        result.get('ckan_relationship'), result.get('molecule_package_id'))
    return result

def _import_molecule_images(package):
    package_id = package['id']
    try:
        if package.get('inchi') and package.get('inchi_key'):
            standard_inchi = package.get('inchi')
            inchi_key = package.get('inchi_key')

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

        elif package.get('inchi'):
            standard_inchi = package.get('inchi')
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
        log.error("Molecule image import failed for package %s: %s", package_id, e)
        return 0


# Used only in CKAN < 2.9
class _UploadLocalFileStorage(cgi.FieldStorage):
    def __init__(self, fp, *args, **kwargs):
        self.name = fp.name
        self.filename = fp.name
        self.file = fp
