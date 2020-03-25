from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jwt import encode as jwt_encode, decode as jwt_decode, PyJWTError
from passlib.context import CryptContext
from starlette.requests import Request
from starlette.status import HTTP_403_FORBIDDEN

from app.auth.models import TokenData, User
from app.config import JWT_ENCODING_ALGORITHM, SECRET_KEY
from app.db.core import get_repository_from_request
from app.db.user import UserRepository

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


async def authenticate_user(request: Request, username: str, password: str):
    user_repo = await get_repository_from_request(request, UserRepository)
    user = await user_repo.get_user(username=username.lower())
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(*, data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({'exp': expire})
    encoded_jwt = jwt_encode(to_encode, SECRET_KEY, algorithm=JWT_ENCODING_ALGORITHM)
    return encoded_jwt


async def get_current_user(request: Request, token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
    )
    try:
        payload = jwt_decode(token, SECRET_KEY, algorithms=[JWT_ENCODING_ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except PyJWTError:
        raise credentials_exception
    user_repo = await get_repository_from_request(request, UserRepository)
    user = await user_repo.get_user(username=token_data.username.lower())
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
