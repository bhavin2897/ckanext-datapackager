# coding=utf-8
import json
import logging


log = logging.getLogger(__name__)


try:
    json_parse_exception = json.decoder.JSONDecodeError
except AttributeError:  # Testing against Python 2
    json_parse_exception = ValueError

resource_mapping = {
    'bytes': 'size',
    'mediatype': 'mimetype',
    'path': 'url'
}

package_mapping = {
    'description': 'notes',
    'homepage': 'url',
}

# Any key not in this list is passed as is inside "extras".
# Further processing will happen for possible matchings, e.g.
# contributor <=> author
ckan_package_keys = [
    'author',
    'author_email',
    'creator_user_id',
    'groups',
    'identifier',
    'license_id',
    'license_title',
    'license',
    'maintainer',
    'maintainer_email',
    'metadata_created',
    'metadata_modified',
    'name',
    'notes',
    'owner_org',
    'private',
    'relationships_as_object',
    'relationships_as_subject',
    'revision_id',
    'resources',
    'state',
    'tags',
    'tracking_summary',
    'title',
    'type',
    'url',
    'version',
    'measurement_technique',
    'measurement_technique_iri',
    'inchi',
    'exactmass',
    'inchi_key',
    'smiles',
    'mol_formula',
    'doi',
    'version',
    'language',
    'metadata_published',
    'alternateNames'
]

frictionless_package_keys_to_exclude = [
    'extras'
]


def _extract_resources(content):
    """
    extract resources from the given dict
    """
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


def package(fddict):
    """Convert a MassBank Import package to a CKAN package (dataset).
    """

    outdict = dict(fddict)

    log.debug(f"{fddict}")
    try:
        if fddict['inChI']:
            outdict['inchi'] = fddict['inChI']
        else:
            outdict['inchi'] = ''

        if fddict['inChIKey']:
            outdict['inchi_key'] = fddict['inChIKey']
        else:
            outdict['inchi_key'] = ''

        if fddict['smiles']:
            outdict['smiles'] = fddict['smiles']
        else:
            outdict['smiles'] = ''

        if fddict['molecularFormula']:
            outdict['mol_formula'] = fddict['molecularFormula']

        elif fddict['chemicalComposition']:
            outdict['mol_formula'] = fddict['chemicalComposition']
        else:
            outdict['mol_formula'] = '-'

        outdict['exactmass'] = fddict['monoisotopicMolecularWeight']

    except Exception as e:
        log.error(f'Missing Chemical Information and {e}')
        pass

    outdict['metadata_published'] = fddict['datePublished']
    # map resources inside dataset

    if 'url' in fddict:
        outdict['resources'] = _extract_resources(fddict)

    if 'license' in fddict:
        outdict['license'] = fddict['license']

    try:
        outdict['title'] = fddict['name']
        outdict['name'] = fddict['identifier'].lower()
        outdict['notes'] = fddict['description']

        if fddict['measurementTechnique']:
            measurement_technique = fddict['measurementTechnique']
            outdict['measurement_technique'] = measurement_technique[0]['name']
            outdict['measurement_technique_iri'] = measurement_technique[0]['url']

        #if 'licenses' in outdict and outdict['licenses']:
        #    outdict['license_id'] = outdict['licenses'][0].get('name')
        #    outdict['license_title'] = outdict['licenses'][0].get('title')
        #    outdict['license_url'] = outdict['licenses'][0].get('path')
        #    # remove it so it won't get put in extras
        #    if len(outdict['licenses']) == 1:
        #        outdict.pop('licenses', None)

        if outdict.get('contributors'):
            for c in outdict['contributors']:
                if c.get('role') in [None, 'author']:
                    outdict['author'] = c.get('title')
                    outdict['author_email'] = c.get('email')
                    break

            for c in outdict['contributors']:
                if c.get('role') == 'maintainer':
                    outdict['maintainer'] = c.get('title')
                    outdict['maintainer_email'] = c.get('email')
                    break

            # we remove contributors where we have extracted everything into
            # ckan core that way it won't end up in extras
            # this helps ensure that round tripping with ckan is good
            # when have we extracted everything?
            # if contributors has length 1 and role in author or maintainer
            # or contributors == 2 and no of authors and maintainer types <= 1
            if (
                    (len(outdict.get('contributors')) == 1 and
                     outdict['contributors'][0].get('role') in [None, 'author',
                                                                'maintainer'])
                    or
                    (len(outdict.get('contributors')) == 2 and
                     [c.get('role') for c in outdict['contributors']]
                     not in (
                             [None, None],
                             ['maintainer', 'maintainer'],
                             ['author', 'author']))
            ):
                outdict.pop('contributors', None)

    except KeyError as e:
        log.debug(e)

    final_dict = dict(outdict)

    for key, value in outdict.items():

        if (
                key not in ckan_package_keys and
                key not in frictionless_package_keys_to_exclude
        ):

            #if isinstance(value, (dict, list)):
            #    value = json.dumps(value)
            #if not final_dict.get('extras'):
            #   final_dict['extras'] = []
            #final_dict['extras'].append(
            #    {'key': key, 'value': value}
            #)

            del final_dict[key]

    outdict = dict(final_dict)

    return outdict


