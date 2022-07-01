from app.models.auth import UserWithPermissions
from app.utils import nested_key_exists
from fastapi import HTTPException
from starlette.status import HTTP_403_FORBIDDEN


def _raise_unauthorized_exception():
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Unauthorized")


def require_user(
    user: UserWithPermissions,
) -> None:
    if user is None or user.disabled:
        _raise_unauthorized_exception()


def require_entity_type_permission(
    user: UserWithPermissions,
    project_name: str,
    entity_type_name: str,
    scope: str,
    permission: str,
) -> None:
    require_user(user)

    if not nested_key_exists(
        user.permissions, project_name, "entities", entity_type_name, scope, permission
    ):
        _raise_unauthorized_exception()


def require_project_permission(
    user: UserWithPermissions,
    project_name: str,
    permission: str,
) -> None:
    # TODO: implement
    raise Exception("Not implemented yet.")
