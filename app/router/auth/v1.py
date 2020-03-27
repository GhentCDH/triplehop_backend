from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from starlette.requests import Request

from app.auth.core import authenticate_user, create_access_token, get_current_active_user
from app.auth.models import Token, User
from app.config import ACCESS_TOKEN_EXPIRE_MINUTES

router = APIRouter()


@router.post('/token', response_model=Token)
async def route_login_access_token(request: Request, form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(request, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail='Incorrect username or password')
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={'sub': user.username}, expires_delta=access_token_expires
    )
    return {'access_token': access_token, 'token_type': 'bearer'}


@router.get('/user', response_model=User)
async def read_user(current_user: User = Depends(get_current_active_user)):
    return current_user

# TODO: password recovery via e-mail
# TODO: password update
# TODO: allow registration / adding users
