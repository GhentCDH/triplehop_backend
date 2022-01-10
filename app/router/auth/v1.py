import datetime
from fastapi import APIRouter, Depends
from fastapi_jwt_auth import AuthJWT
from starlette.requests import Request

from app.mgmt.auth import AuthManager, get_current_active_user_with_permissions
from app.models.auth import FormUser, Tokens, UserWithPermissions, UserWithPermissionsResponse

router = APIRouter()


@router.post('/login', response_model=Tokens)
async def login(request: Request, user: FormUser, Authorize: AuthJWT = Depends()):
    auth_manager = AuthManager(request)
    user = await auth_manager.authenticate_user(user.username, user.password)
    # TODO: save tokens in app.user

    return {
        'access_token': Authorize.create_access_token(subject=user.username),
        'refresh_token': Authorize.create_refresh_token(subject=user.username),
    }


@router.post('/refresh', response_model=Tokens)
async def refresh(Authorize: AuthJWT = Depends()):
    Authorize.jwt_refresh_token_required()

    # TODO: save tokens in app.user

    tokens = {
        'access_token': Authorize.create_access_token(subject=Authorize.get_jwt_subject()),
    }

    # Create new refresh token if almost expired
    untill_expiration = datetime.datetime.fromtimestamp(Authorize.get_raw_jwt()['exp']) - datetime.datetime.now()
    if untill_expiration < 10 * Authorize._access_token_expires:
        tokens['refresh_token'] = Authorize.create_refresh_token(subject=Authorize.get_jwt_subject())

    return tokens


@router.get('/logout', response_model=None)
async def logout(request: Request, Authorize: AuthJWT = Depends()):
    Authorize.jwt_required()

    # TODO: get tokens from app.user
    # TODO: add these tokens to denylist
    # TODO: delete tokens from app.user

    auth_manager = AuthManager(request)
    await auth_manager.revoke_token(Authorize)

    return


@router.get('/user', response_model=UserWithPermissionsResponse)
async def user(user_with_permissions: UserWithPermissions = Depends(get_current_active_user_with_permissions)):
    return {
        'user': user_with_permissions,
    }

# TODO: password recovery via e-mail
# TODO: password update
# TODO: allow registration / adding users
