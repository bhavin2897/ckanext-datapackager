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

AUTHOR_DELIMITER = '; '


def _extract_people(value):
    """Normalize supported Schema.org Person/Organization representations."""
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        value = [value]

    people = []
    seen_names = set()
    for person in value:
        email = None
        if isinstance(person, str):
            name = person.strip()
        elif isinstance(person, dict):
            name = person.get('name')
            if not name:
                name = ' '.join(
                    part.strip() for part in (
                        person.get('givenName') or '',
                        person.get('familyName') or '')
                    if isinstance(part, str) and part.strip())
            if not name:
                name = person.get('@id')
            name = name.strip() if isinstance(name, str) else ''
            email = person.get('email')
            email = email.strip() if isinstance(email, str) else None
        else:
            continue

        if not name or name in seen_names:
            continue
        seen_names.add(name)
        people.append({'name': name, 'email': email or None})
    return people


def _legacy_contributors(contributors, roles):
    if not isinstance(contributors, list):
        return []
    selected = []
    for contributor in contributors:
        if not isinstance(contributor, dict):
            continue
        role = contributor.get('role')
        role = role.lower() if isinstance(role, str) else role
        if role in roles:
            # Older Frictionless contributors use title instead of name.
            contributor = dict(contributor)
            contributor['name'] = contributor.get('name') or contributor.get('title')
            selected.append(contributor)
    return _extract_people(selected)


def _apply_people_metadata(outdict, fddict):
    author_people = []
    for source_key in ('creator', 'author', 'contributor'):
        author_people = _extract_people(fddict.get(source_key))
        if author_people:
            break
    if not author_people:
        author_people = _legacy_contributors(
            fddict.get('contributors'), (None, 'author'))

    if author_people:
        names = [person['name'] for person in author_people]
        outdict['author'] = AUTHOR_DELIMITER.join(names)
        if len(author_people) == 1 and author_people[0].get('email'):
            outdict['author_email'] = author_people[0]['email']
        log.info("Mapped Dataset %s authors=%s",
                 fddict.get('identifier'), AUTHOR_DELIMITER.join(names))

    maintainers = _legacy_contributors(
        fddict.get('contributors'), ('maintainer',))
    if maintainers:
        outdict['maintainer'] = AUTHOR_DELIMITER.join(
            person['name'] for person in maintainers)
        if len(maintainers) == 1 and maintainers[0].get('email'):
            outdict['maintainer_email'] = maintainers[0]['email']


def _extract_resources(content):
    """
    extract resources from the given dict
    """
    resources = []
    url = content.get('url')
    log.debug("URL of resource: %s" % url)
    if url:
        try:
            resource_format = content["format"][0]
        except (IndexError, KeyError):
            resource_format = "HTML"
        resources.append(
            {
                "name": content.get("name") or content.get("identifier"),
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

    log.debug("Mapping Schema.org Dataset %s", fddict.get('identifier'))
    outdict['inchi'] = fddict.get('inChI', '')
    outdict['inchi_key'] = fddict.get('inChIKey', '')
    outdict['smiles'] = fddict.get('smiles', '')
    outdict['mol_formula'] = (
        fddict.get('molecularFormula') or
        fddict.get('chemicalComposition') or
        ''
    )
    if fddict.get('monoisotopicMolecularWeight') is not None:
        outdict['exactmass'] = fddict.get('monoisotopicMolecularWeight')

    if fddict.get('datePublished') is not None:
        outdict['metadata_published'] = fddict.get('datePublished')
    # map resources inside dataset

    if 'url' in fddict:
        outdict['resources'] = _extract_resources(fddict)

    if 'license' in fddict:
        outdict['license'] = fddict['license']

    try:
        identifier = fddict.get('identifier') or fddict.get('name')
        if identifier:
            outdict['identifier'] = identifier
            outdict['name'] = identifier.lower()
        if fddict.get('name'):
            outdict['title'] = fddict.get('name')
        if fddict.get('description') is not None:
            outdict['notes'] = fddict.get('description')

        measurement_technique = fddict.get('measurementTechnique')
        if measurement_technique:
            if isinstance(measurement_technique, dict):
                measurement_technique = [measurement_technique]
            technique = measurement_technique[0]
            if technique.get('name'):
                outdict['measurement_technique'] = technique.get('name')
            if technique.get('url'):
                outdict['measurement_technique_iri'] = technique.get('url')

        #if 'licenses' in outdict and outdict['licenses']:
        #    outdict['license_id'] = outdict['licenses'][0].get('name')
        #    outdict['license_title'] = outdict['licenses'][0].get('title')
        #    outdict['license_url'] = outdict['licenses'][0].get('path')
        #    # remove it so it won't get put in extras
        #    if len(outdict['licenses']) == 1:
        #        outdict.pop('licenses', None)

        _apply_people_metadata(outdict, fddict)
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


