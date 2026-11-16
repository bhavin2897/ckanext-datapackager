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


def object_with_read():
    class Upload(object):
        def read(self):
            return b''
    return Upload()
