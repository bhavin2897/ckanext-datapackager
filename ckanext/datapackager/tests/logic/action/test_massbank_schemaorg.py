import copy

import pytest

import ckan.plugins.toolkit as toolkit
from ckanext.datapackager.logic.action import create
from ckanext.datapackager.logic.action import schema4chem_ckan_mapper as mapper


IDENTIFIER = 'MSBNK-ACES_SU-AS000305'


def _dataset(identifier=IDENTIFIER, **values):
    result = {
        '@type': 'Dataset',
        'identifier': identifier,
        'name': 'MassBank spectrum',
        'description': 'A spectrum',
        'datePublished': '2025-06-26',
    }
    result.update(values)
    return result


def _substance(identifier=IDENTIFIER, part=None, **values):
    result = {
        '@type': 'ChemicalSubstance',
        'identifier': identifier,
        'name': 'Ambroxol',
        'hasBioChemEntityPart': part if part is not None else {
            '@type': 'MolecularEntity',
            'inChI': 'InChI=1S/example',
            'inChIKey': 'EXAMPLE-KEY',
            'smiles': 'CC',
            'molecularFormula': 'C2H6',
            'monoisotopicMolecularWeight': 30.04695,
        },
    }
    result.update(values)
    return result


def _mapped(objects):
    prepared = create._prepare_schemaorg_datasets(objects)
    return [mapper.package(item) for item in prepared]


@pytest.mark.parametrize('as_list', [False, True])
def test_dataset_and_nested_molecular_entity_map_to_one_package(as_list):
    substance = _substance()
    if as_list:
        substance['hasBioChemEntityPart'] = [substance['hasBioChemEntityPart']]

    packages = _mapped([_dataset(), substance])

    assert len(packages) == 1
    assert packages[0]['identifier'] == IDENTIFIER
    assert packages[0]['name'] == 'msbnk-aces_su-as000305'
    assert packages[0]['inchi'] == 'InChI=1S/example'
    assert packages[0]['inchi_key'] == 'EXAMPLE-KEY'
    assert packages[0]['smiles'] == 'CC'
    assert packages[0]['mol_formula'] == 'C2H6'
    assert packages[0]['exactmass'] == 30.04695


def test_chemical_composition_falls_back_when_molecular_formula_is_missing():
    entity = _substance()['hasBioChemEntityPart']
    entity.pop('molecularFormula')
    package = _mapped([_dataset(), _substance(part=entity, chemicalComposition='C13H18Br2N2O')])[0]
    assert package['mol_formula'] == 'C13H18Br2N2O'


def test_missing_optional_dataset_and_chemical_fields_are_safe():
    dataset = _dataset()
    dataset.pop('datePublished')
    package = _mapped([dataset, _substance(part={'@type': 'MolecularEntity'})])[0]
    assert 'metadata_published' not in package
    assert package['inchi'] == ''
    assert package['mol_formula'] == ''
    assert 'measurement_technique' not in package


def test_dataset_without_chemical_substance_is_supported():
    assert _mapped([_dataset()])[0]['identifier'] == IDENTIFIER


def test_orphan_chemical_substance_has_clear_validation_error():
    with pytest.raises(toolkit.ValidationError, match='exactly one Dataset'):
        _mapped([_substance()])


def test_pairing_is_case_insensitive_and_preserves_dataset_identifier():
    package = _mapped([_dataset(), _substance(identifier=IDENTIFIER.lower())])[0]
    assert package['identifier'] == IDENTIFIER


def test_old_flat_dataset_format_remains_supported():
    package = _mapped([_dataset(
        inChI='flat-inchi', inChIKey='flat-key', smiles='CO',
        molecularFormula='CH4O', monoisotopicMolecularWeight=32.0262,
    )])[0]
    assert package['inchi'] == 'flat-inchi'
    assert package['mol_formula'] == 'CH4O'


def test_existing_package_without_mol_formula_is_updated_safely(monkeypatch):
    existing = {'id': 'package-id', 'name': 'package-id', 'license_id': None}

    def get_action(action):
        if action == 'package_show':
            return lambda context, data: copy.deepcopy(existing)
        if action == 'license_list':
            return lambda context, data: []
        raise AssertionError(action)

    monkeypatch.setattr(create.toolkit, 'get_action', get_action)
    result = create._handle_existing_package({}, {
        'id': 'package-id', 'mol_formula': 'C2H6', 'inchi': 'new-inchi'})
    assert result['mol_formula'] == 'C2H6'
    assert result['inchi'] == 'new-inchi'


def test_validation_finishes_before_any_package_create(monkeypatch):
    calls = []

    class Item(object):
        def __init__(self, value):
            self.value = value

        def to_dict(self):
            return self.value

    monkeypatch.setattr(
        create, '_load_and_validate_datapackage',
        lambda **kwargs: [Item(_dataset()), Item(_dataset())])
    monkeypatch.setattr(
        create.toolkit, 'get_action',
        lambda action: calls.append(action) or (lambda context, data: {}))

    with pytest.raises(toolkit.ValidationError, match='exactly one Dataset'):
        create.package_create_from_datapackage({}, {'upload': object_with_read()})
    assert 'package_create' not in calls


@pytest.mark.parametrize('creator, expected', [
    ('Alice Example', 'Alice Example'),
    ({'name': 'Alice Example'}, 'Alice Example'),
    ({'givenName': 'Alice', 'familyName': 'Example'}, 'Alice Example'),
])
def test_schemaorg_creator_forms_map_to_author(creator, expected):
    package = mapper.package(_dataset(creator=creator))
    assert package['author'] == expected


def test_creator_person_list_preserves_order_and_uses_schema_delimiter():
    package = mapper.package(_dataset(creator=[
        {'@type': 'Person', 'name': 'Alice Example'},
        {'@type': 'Organization', 'name': 'Example Laboratory'},
    ]))
    assert package['author'] == 'Alice Example; Example Laboratory'
    assert 'author_email' not in package


def test_legacy_author_and_maintainer_are_mapped_separately():
    package = mapper.package(_dataset(contributors=[
        {'title': 'Legacy Author', 'email': 'author@example.test',
         'role': 'author'},
        {'title': 'Dataset Maintainer', 'email': 'maintainer@example.test',
         'role': 'maintainer'},
    ]))
    assert package['author'] == 'Legacy Author'
    assert package['author_email'] == 'author@example.test'
    assert package['maintainer'] == 'Dataset Maintainer'
    assert package['maintainer_email'] == 'maintainer@example.test'


def test_duplicate_people_are_removed_without_empty_separators():
    package = mapper.package(_dataset(creator=[
        {'name': 'Alice Example'}, None, {'name': ''},
        {'name': 'Alice Example'}, {'name': 'Bob Example'},
    ]))
    assert package['author'] == 'Alice Example; Bob Example'


def test_missing_people_metadata_is_safe():
    package = mapper.package(_dataset())
    assert 'author' not in package
    assert 'author_email' not in package


def test_repeated_author_mapping_is_stable():
    dataset = _dataset(creator=[{'name': 'Alice'}, {'name': 'Alice'}])
    assert mapper.package(dataset)['author'] == 'Alice'
    assert mapper.package(dataset)['author'] == 'Alice'


def test_existing_package_merges_author_and_preserves_unrelated_data(monkeypatch):
    existing = {
        'id': 'package-id', 'name': 'package-id', 'license_id': 'cc-by',
        'resources': [{'id': 'resource-id'}], 'groups': [{'id': 'group-id'}],
        'tags': [{'name': 'existing'}], 'related_molecule': ['molecule-old'],
    }
    monkeypatch.setattr(
        create.toolkit, 'get_action',
        lambda action: (lambda context, data: copy.deepcopy(existing)))
    result = create._handle_existing_package({}, {
        'id': 'package-id', 'author': 'Alice Example',
        'inchi_key': 'EXAMPLE-KEY'})
    assert result['author'] == 'Alice Example'
    assert result['inchi_key'] == 'EXAMPLE-KEY'
    assert result['resources'] == existing['resources']
    assert result['groups'] == existing['groups']
    assert result['tags'] == existing['tags']
    assert result['related_molecule'] == ['molecule-old']


@pytest.mark.parametrize('molecule_status, relation_status', [
    ('existing', 'existing'),
    ('created', 'created'),
])
def test_canonical_molecule_synchronizer_is_used(
        molecule_status, relation_status):
    calls = []

    def synchronizer(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            'molecule_package': molecule_status,
            'molecule_package_id': 'molecule-package-id',
            'ckan_relationship': relation_status,
        }

    package = {
        'id': 'dataset-id', 'identifier': IDENTIFIER,
        'inchi': 'InChI=1S/example', 'inchi_key': 'EXAMPLE-KEY',
        'smiles': 'CC', 'mol_formula': 'C2H6', 'exactmass': 30.04695,
    }
    result = create._synchronize_related_molecule(
        package, ['Example molecule'], synchronizer)
    assert result['molecule_package'] == molecule_status
    assert result['ckan_relationship'] == relation_status
    assert calls[0][0][:3] == (
        'dataset-id', 'InChI=1S/example', 'EXAMPLE-KEY')
    assert calls[0][1]['names'] == ['Example molecule']


def test_missing_inchi_key_skips_molecule_synchronization():
    calls = []
    assert create._synchronize_related_molecule(
        {'id': 'dataset-id'}, synchronizer=calls.append) is None
    assert calls == []


@pytest.mark.parametrize('part', [None, [], {}, 'malformed'])
def test_empty_or_malformed_biochemical_part_is_safe(part):
    substance = _substance(part={'@type': 'MolecularEntity'})
    substance['hasBioChemEntityPart'] = part
    if part == 'malformed':
        with pytest.raises(toolkit.ValidationError, match='Expected an object or list'):
            _mapped([_dataset(), substance])
    else:
        assert len(_mapped([_dataset(), substance])) == 1


def test_import_synchronizes_before_active_update_and_returns_relationship(
        monkeypatch):
    events = []

    class Item(object):
        def __init__(self, value):
            self.value = value

        def to_dict(self):
            return self.value

    dataset = _dataset(creator='Alice Example', license='cc-by')
    monkeypatch.setattr(
        create, '_load_and_validate_datapackage',
        lambda **kwargs: [Item(dataset), Item(_substance())])
    monkeypatch.setattr(create, '_find_existing_package',
                        lambda package, context: None)

    def create_package(context, incoming):
        events.append('package_create')
        result = copy.deepcopy(incoming)
        result.update({'id': 'dataset-id',
                       'resources': [{'id': 'resource-id'}]})
        return result

    def synchronize(package, names):
        events.append('molecule_sync')
        assert package['author'] == 'Alice Example'
        assert package['state'] == 'active'
        assert names[0] == 'Ambroxol'

    def get_action(action):
        assert action == 'package_update'

        def update(context, incoming):
            events.append('package_update')
            result = copy.deepcopy(incoming)
            result['related_molecule'] = ['molecule-package-id']
            return result
        return update

    monkeypatch.setattr(create, '_package_create_with_unique_name',
                        create_package)
    monkeypatch.setattr(create, '_synchronize_related_molecule', synchronize)
    monkeypatch.setattr(create, '_import_molecule_images', lambda package: None)
    monkeypatch.setattr(create.toolkit, 'get_action', get_action)

    result = create.package_create_from_datapackage(
        {}, {'upload': object_with_read()})
    assert events == ['package_create', 'molecule_sync', 'package_update']
    assert result[0]['related_molecule'] == ['molecule-package-id']
    assert result[0]['state'] == 'active'


def object_with_read():
    class Upload(object):
        def read(self):
            return b''
    return Upload()
