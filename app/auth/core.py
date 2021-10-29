from fastapi import Depends, HTTPException
from fastapi_jwt_auth import AuthJWT
from passlib.context import CryptContext
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.auth import AuthRepository
from app.models.auth import User, UserWithPermissions

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


async def authenticate_user(
    request: Request,
    username: str,
    password: str,
):
    auth_repo = get_repository_from_request(request, AuthRepository)
    user = await auth_repo.get_user(username=username.lower())
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return User(**user.dict())


async def get_current_active_user_with_permissions(
    request: Request,
    Authorize: AuthJWT = Depends(),
):
    Authorize.jwt_required()

    auth_repo = get_repository_from_request(request, AuthRepository)

    # fastapi-jwt-auth deny list can't be used because of
    # https://github.com/IndominusByte/fastapi-jwt-auth/issues/30
    await auth_repo.denylist_purge_expired_tokens()
    if not await auth_repo.denylist_check_token(Authorize.get_raw_jwt()['jti']):
        raise HTTPException(status_code=401, detail='Inactive token')

    user = await auth_repo.get_user(username=Authorize.get_jwt_subject())
    if user.disabled:
        raise HTTPException(status_code=401, detail='Inactive user')

    permissions = await auth_repo.get_permissions(user=user)
    return UserWithPermissions(**user.dict(), permissions=permissions)


async def revoke_token(
    request: Request,
    Authorize: AuthJWT,
):
    # TODO: check if it is an access token or refresh token; adopt expiration accordingly
    auth_repo = get_repository_from_request(request, AuthRepository)
    await auth_repo.denylist_add_token(Authorize.get_raw_jwt()['jti'], Authorize._access_token_expires.seconds)
