from fastapi import APIRouter, Depends, HTTPException
from fastapi_jwt_auth import AuthJWT
from starlette.requests import Request

from app.auth.core import authenticate_user, get_current_active_user_with_permissions
from app.models.auth import FormUser, Token, UserWithPermissionsResponse

router = APIRouter()


@router.post('/login', response_model=Token)
async def login(request: Request, user: FormUser, Authorize: AuthJWT = Depends()):
    print('login')
    user = await authenticate_user(request, user.username, user.password)
    if not user:
        raise HTTPException(status_code=400, detail='Incorrect username or password')
    print(user)
    access_token = Authorize.create_access_token(subject=user.username)
    print(access_token)
    return {
        'access_token': access_token,
    }


@router.get('/user', response_model=UserWithPermissionsResponse)
async def user(request: Request, Authorize: AuthJWT = Depends()):
    Authorize.jwt_required()

    return {
        'user': await get_current_active_user_with_permissions(request, Authorize.get_jwt_subject()),
    }

# TODO: password recovery via e-mail
# TODO: password update
# TODO: allow registration / adding users
