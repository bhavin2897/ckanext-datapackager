# encoding: utf-8

import ckan.logic as logic
import ckan.authz as authz
from ckan.logic.auth import get_group_object
from ckan.logic.auth import get_resource_object
from ckan.common import _
from ckan.types import Context, DataDict, AuthResult


def purge_dataset_foreignkeys(context: Context, data_dict: DataDict) -> AuthResult:
    # Only sysadmins are authorized to purge datasets
    return {'success': False}