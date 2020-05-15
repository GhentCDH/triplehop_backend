from fastapi import HTTPException
from starlette.status import HTTP_403_FORBIDDEN

from app.models.auth import UserWithPermissions


def _raise_unauthorized_exception():
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail='Unauthorized')


def require_entity_permission(
    user: UserWithPermissions,
    project_name: str,
    entity_type_name: str,
    permission: str,
) -> None:
    permissions = user.permissions

    if permission not in permissions:
        _raise_unauthorized_exception()

    if (
        '__all__' in permissions[permission]
        and (
            '__all__' in permissions[permission]['__all__']['entities']
            or entity_type_name in permissions[permission]['__all__']['entities']
        )
    ):
        return

    if (
        project_name in permissions[permission]
        and (
            '__all__' in permissions[permission][project_name]['entities']
            or entity_type_name in permissions[permission][project_name]['entities']
        )
    ):
        return

    _raise_unauthorized_exception()


def require_global_permission(
    user: UserWithPermissions,
    permission: str,
) -> None:
    permissions = user.permissions

    if permission not in permissions:
        _raise_unauthorized_exception()

    if (
        '__all__' in permissions[permission]
        and '__all__' in permissions[permission]['__all__']['entities']
        and '__all__' in permissions[permission]['__all__']['relations']
    ):
        return

    _raise_unauthorized_exception()


def require_project_permission(
    user: UserWithPermissions,
    project_name: str,
    permission: str,
) -> None:
    permissions = user.permissions

    if permission not in permissions:
        _raise_unauthorized_exception()

    if (
        '__all__' in permissions[permission]
        and '__all__' in permissions[permission]['__all__']['entities']
        and '__all__' in permissions[permission]['__all__']['relations']
    ):
        return

    if (
        project_name in permissions[permission]
        and '__all__' in permissions[permission][project_name]['entities']
        and '__all__' in permissions[permission][project_name]['relations']
    ):
        return

    _raise_unauthorized_exception()
