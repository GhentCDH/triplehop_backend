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

    # Check for specific or all on permission, project, entity level
    for perm in [permission, '__all__']:
        for proj in [project_name, '__all__']:
            for ent in [entity_type_name, '__all__']:
                if (
                    perm in permissions
                    and proj in permissions[perm]
                    and ent in permissions[perm][proj]['entities']
                ):
                    return

    _raise_unauthorized_exception()


def require_global_permission(
    user: UserWithPermissions,
    permission: str,
) -> None:
    permissions = user.permissions

    # Check for specific or all on permission level
    for perm in [permission, '__all__']:
        if (
            '__all__' in permissions[perm]
            and '__all__' in permissions[perm]['__all__']['entities']
            and '__all__' in permissions[perm]['__all__']['relations']
        ):
            return

    _raise_unauthorized_exception()


def require_project_permission(
    user: UserWithPermissions,
    project_name: str,
    permission: str,
) -> None:
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
