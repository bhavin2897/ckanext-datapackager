# encoding: utf-8

from __future__ import annotations

# from ckan.types.logic import ActionResult
import logging
from typing import Any, Union, Type, cast

import sqlalchemy as sqla

import ckan.lib.jobs as jobs
import ckan.logic
import ckan.logic.action
import ckan.logic.schema
import ckan.plugins as plugins
import ckan.lib.api_token as api_token
from ckan import authz
from ckan.lib.navl.dictization_functions import validate
from ckan.model.follower import ModelFollowingModel
from ckanext.rdkit_visuals.models.molecule_rel import MolecularRelationData as mol_rel_data
from ckanext.related_resources.models.related_resources import RelatedResources as related_resource_data
from ckan.logic import check_access
from ckan.logic import NotFound


from ckan.common import _

log = logging.getLogger(__name__)

ValidationError = ckan.logic.ValidationError
#NotFound = ckan.logic.NotFound
#_check_access = ckan.logic.check_access
_get_or_bust = ckan.logic.get_or_bust
_get_action = ckan.logic.get_action


def purge_dataset_foreignkeys(context, data_dict):
    """
    Purge foreignkeys from the related tables, that have been developed, modelled and migrated.

    Here in this deletion of foreignkeys of tables from molecules and molecule_rel_data rows will be purged, along
    the dataset.

    We need to use this function to delete the rows in these tables, and then use "dataset_purge" to permanently
    delete the datasets from database.

     :param id: the name or the id of the dataset
     :type id: string
    """

    model = context['model']
    id = _get_or_bust(data_dict, 'id')
    type = _get_or_bust(data_dict, 'type')

    pkg = model.Package.get(id)

    try:
        check_access('sysadmin', context, data_dict)
    except NotFound:
        raise NotFound('Only sysadmin can access this function.')

    if pkg is None:
        raise NotFound('Dataset not found')

    context['package'] = pkg

    molecule_id_members = model.Session.query(mol_rel_data).filter(mol_rel_data.package_id == pkg.id)
    related_resources_members = model.Session.query(related_resource_data).filter(related_resource_data.package_id == pkg.id)

    if molecule_id_members.count() > 0:
        for row in molecule_id_members.all():
            log.debug(f'Purging dataset id: {row.package_id}')
            model.Session.delete(row)

    if related_resources_members.count() > 0:
        for row in related_resources_members.all():
            log.debug(f'Purging related resource... ')
            model.Session.delete(row)

    pkg = model.Package.get(id)
    assert pkg
    pkg.purge()
    model.repo.commit_and_remove()
