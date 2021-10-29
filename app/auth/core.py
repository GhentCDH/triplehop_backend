from fastapi import HTTPException
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext
from starlette.requests import Request

from app.db.core import get_repository_from_request
from app.db.permission import PermissionRepository
from app.db.user import UserRepository
from app.models.auth import User, UserWithPermissions

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')
oauth2_scheme = OAuth2PasswordBearer(tokenUrl='/token')


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


async def authenticate_user(request: Request, username: str, password: str):
    user_repo = get_repository_from_request(request, UserRepository)
    user = await user_repo.get_user(username=username.lower())
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return User(**user.dict())


async def get_current_active_user_with_permissions(
    request: Request,
    username: str
):
    user_repo = get_repository_from_request(request, UserRepository)
    user = await user_repo.get_user(username=username.lower())

    if user.disabled:
        raise HTTPException(status_code=400, detail='Inactive user')

    permission_repo = get_repository_from_request(request, PermissionRepository)
    permissions = await permission_repo.get_permissions(user=user)
    return UserWithPermissions(**user.dict(), permissions=permissions)
