from base64 import b64decode
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from jwt import encode as jwt_encode
from passlib.context import CryptContext
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

from app.authentication import BasicAuth
from app.db.core import get_repository_from_request
from app.db.user import UserRepository

# TODO: read from config file
ACCESS_TOKEN_EXPIRE_MINUTES = 30
SECRET_KEY = '946802f95a3a8bb2d2cfdf2e21c14c604d7f2bf68f54f2db03475f9cf18ec89e'
JWT_ENCODING_ALGORITHM = 'HS256'

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
router = APIRouter()


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


async def authenticate_user(request: Request, email: str, password: str):
    user_repo = await get_repository_from_request(request, UserRepository)
    user = user_repo.get_user(email)
    if not user:
        return False
    if not verify_password(password, user['hashed_password']):
        return False
    return user


def create_access_token(*, data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt_encode(to_encode, SECRET_KEY, algorithm=JWT_ENCODING_ALGORITHM)
    return encoded_jwt


@router.get("/login_basic")
async def login_basic(request: Request, auth: BasicAuth = Depends(BasicAuth(auto_error=False))):
    if not auth:
        response = Response(headers={"WWW-Authenticate": "Basic"}, status_code=401)
        return response

    try:
        decoded = b64decode(auth).decode("ascii")
        username, _, password = decoded.partition(":")
        user = await authenticate_user(request, username, password)
        if not user:
            raise HTTPException(status_code=400, detail="Incorrect email or password")

        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": username}, expires_delta=access_token_expires
        )

        token = jsonable_encoder(access_token)

        # TODO: redirect to home page or referer
        response = RedirectResponse(url="/docs")
        response.set_cookie(
            "Authorization",
            value=f"Bearer {token}",
            domain="localtest.me",
            httponly=True,
            max_age=1800,
            expires=1800,
        )
        return response

    except:
        response = Response(headers={"WWW-Authenticate": "Basic"}, status_code=401)
        return response
