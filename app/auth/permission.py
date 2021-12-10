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


def _permission_usage_helper(
    user: UserWithPermissions,
    project_name: str,
    type_config: typing.Dict,
    permission: str,
    entities_or_relations: str,
) -> typing.Dict[str, typing.List[str]]:
    if user is None or user.disabled:
        return []

    permissions = user.permissions

    usage = {}

    # __all__ on entity level => return all entities and props
    for perm in [permission, '__all__']:
        for proj in [project_name, '__all__']:
            if (
                perm in permissions
                and proj in permissions[perm]
                and '__all__' in permissions[perm][proj][entities_or_relations]
            ):
                for type_name, conf in type_config.items():
                    if 'data' in conf['config']:
                        props = [prop['system_name'] for prop in conf['config']['data'].values()]
                        # only global admins can update ids
                        if has_global_permission(user, permission):
                            props.append('id')
                        usage[type_name] = props
                    else:
                        usage[type_name] = []
                return usage

    # individual entities
    for type_name, conf in type_config.items():
        if (
            permission in permissions
            and project_name in permissions[perm]
            and type_name in permissions[perm][proj][entities_or_relations]
        ):
            if permissions[perm][proj][entities_or_relations][type_name] == '__all__':
                if 'data' in conf['config']:
                    usage[type_name] = [prop['system_name'] for prop in conf['config']['data']]
                else:
                    usage[type_name] = []
            else:
                usage[type_name] = permissions[perm][proj][entities_or_relations][type_name]

    return usage


def get_permission_entities_and_properties(
    user: UserWithPermissions,
    project_name: str,
    entity_types_config: typing.Dict,
    permission: str,
) -> typing.Dict[str, typing.List[str]]:
    return _permission_usage_helper(
        user,
        project_name,
        entity_types_config,
        permission,
        'entities',
    )


def get_permission_relations_and_properties(
    user: UserWithPermissions,
    project_name: str,
    relation_types_config: typing.Dict,
    permission: str,
) -> typing.Dict[str, typing.List[str]]:
    return _permission_usage_helper(
        user,
        project_name,
        relation_types_config,
        permission,
        'relations',
    )


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
