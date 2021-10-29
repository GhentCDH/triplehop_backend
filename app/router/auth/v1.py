from fastapi import APIRouter, Depends, HTTPException
from fastapi_jwt_auth import AuthJWT
from starlette.requests import Request

from app.auth.core import authenticate_user, get_current_active_user_with_permissions, revoke_token
from app.models.auth import FormUser, Token, UserWithPermissions, UserWithPermissionsResponse

router = APIRouter()


@router.post('/login', response_model=Token)
async def login(request: Request, user: FormUser, Authorize: AuthJWT = Depends()):
    user = await authenticate_user(request, user.username, user.password)
    if not user:
        raise HTTPException(status_code=400, detail='Incorrect username or password')
    return {
        'access_token': Authorize.create_access_token(subject=user.username),
    }


@router.get('/logout', response_model=None)
async def logout(request: Request, Authorize: AuthJWT = Depends()):
    Authorize.jwt_required()

    await revoke_token(request, Authorize)
    return


@router.get('/user', response_model=UserWithPermissionsResponse)
async def user(user_with_permissions: UserWithPermissions = Depends(get_current_active_user_with_permissions)):
    return {
        'user': user_with_permissions,
    }

# TODO: password recovery via e-mail
# TODO: password update
# TODO: allow registration / adding users
