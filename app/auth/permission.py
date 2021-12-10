from fastapi import HTTPException
from starlette.status import HTTP_403_FORBIDDEN
import typing

from app.models.auth import UserWithPermissions


def _raise_unauthorized_exception():
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail='Unauthorized')


def require_user(
    user: UserWithPermissions,
) -> None:
    if user is None or user.disabled:
        _raise_unauthorized_exception()


def require_entity_permission(
    user: UserWithPermissions,
    project_name: str,
    entity_type_name: str,
    permission: str,
) -> None:
    require_user(user)
    permissions = user.permissions

    # Check for specific or all on permission, project, entity level
    for perm in [permission, '__all__']:
        for proj in [project_name, '__all__']:
            for etn in [entity_type_name, '__all__']:
                if (
                    perm in permissions
                    and proj in permissions[perm]
                    and etn in permissions[perm][proj]['entities']
                ):
                    return

    _raise_unauthorized_exception()


def get_permission_entities_and_properties(
    user: UserWithPermissions,
    project_name: str,
    entity_types_config: typing.Dict,
    permission: str,
) -> typing.Dict[str, typing.List[str]]:
    if user is None or user.disabled:
        return []

    permissions = user.permissions

    entities_and_props = {}

    # __all__ on entity level => return all entities and props
    for perm in [permission, '__all__']:
        for proj in [project_name, '__all__']:
            if (
                perm in permissions
                and proj in permissions[perm]
                and '__all__' in permissions[perm][proj]['entities']
            ):
                for etn, conf in entity_types_config.items():
                    if 'data' in conf['config']:
                        props = [prop['system_name'] for prop in conf['config']['data'].values()]
                        # only global admins can update ids
                        if has_global_permission(user, permission):
                            props.append('id')
                        entities_and_props[etn] = props
                    else:
                        entities_and_props[etn] = []
                return entities_and_props

    # individual entities
    for etn, conf in entity_types_config.items():
        if (
            permission in permissions
            and project_name in permissions[perm]
            and etn in permissions[perm][proj]['entities']
        ):
            if permissions[perm][proj]['entities'][etn] == '__all__':
                if 'data' in conf['config']:
                    entities_and_props[etn] = [prop['system_name'] for prop in conf['config']['data']]
                else:
                    entities_and_props[etn] = []
            else:
                entities_and_props[etn] = permissions[perm][proj]['entities'][etn]

    return entities_and_props


def has_global_permission(
    user: UserWithPermissions,
    permission: str,
) -> None:
    if user is None or user.disabled:
        return False
    permissions = user.permissions

    # Check for specific or all on permission level
    for perm in [permission, '__all__']:
        if (
            perm in permissions
            and '__all__' in permissions[perm]
            and '__all__' in permissions[perm]['__all__']['entities']
            and '__all__' in permissions[perm]['__all__']['relations']
        ):
            return True

    return False


def require_global_permission(
    user: UserWithPermissions,
    permission: str,
) -> None:
    if not has_global_permission(user, permission):
        _raise_unauthorized_exception()


def require_project_permission(
    user: UserWithPermissions,
    project_name: str,
    permission: str,
) -> None:
    require_user(user)
    permissions = user.permissions

    # Check for specific or all on permission, project level
    for perm in [permission, '__all__']:
        for proj in [project_name, '__all__']:
            if (
                '__all__' in permissions[perm][proj]['entities']
                and '__all__' in permissions[perm][proj]['relations']
            ):
                return

    _raise_unauthorized_exception()
