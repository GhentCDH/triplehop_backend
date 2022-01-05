from fastapi.exceptions import HTTPException
from fastapi_jwt_auth.auth_jwt import AuthJWT
from fastapi_jwt_auth.exceptions import JWTDecodeError, MissingTokenError
from passlib.context import CryptContext
from starlette.requests import Request

from app.db.auth import AuthRepository
from app.db.config import ConfigRepository
from app.db.core import get_repository_from_request
from app.models.auth import User, UserWithPermissions


class AuthManager:
    def __init__(
        self,
        request: Request,
    ):
        self._auth_repo = get_repository_from_request(request, AuthRepository)
        self._config_repo = get_repository_from_request(request, ConfigRepository)

    async def authenticate_user(
        self,
        username: str,
        password: str,
    ):
        user = await self._auth_repo.get_user(username=username.lower())

        if user is None:
            raise HTTPException(status_code=400, detail='Incorrect username or password')

        pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')
        if not pwd_context.verify(password, user.hashed_password):
            raise HTTPException(status_code=400, detail='Incorrect username or password')

        return User(**user.dict())

    async def get_current_active_user_with_permissions(
        self,
        Authorize: AuthJWT,
    ):
        user = None

        # Validate user credentials
        # If token is missing, load anonymous user
        try:
            Authorize.jwt_required()
        except MissingTokenError:
            user = await self._auth_repo.get_user(username='anonymous')
        except JWTDecodeError:
            raise HTTPException(status_code=401, detail='Could not validate credentials')

        # Validated user
        if user is None:
            # Validate if tokens have been denied
            # fastapi-jwt-auth deny list can't be used because of
            # https://github.com/IndominusByte/fastapi-jwt-auth/issues/30
            await self._auth_repo.denylist_purge_expired_tokens()
            if not await self._auth_repo.denylist_check_token(Authorize.get_raw_jwt()['jti']):
                raise HTTPException(status_code=401, detail='Inactive token')

            # Load user
            user = await self._auth_repo.get_user(username=Authorize.get_jwt_subject())
            if user.disabled:
                raise HTTPException(status_code=401, detail='Inactive user')

        # Anonymous users can have permissions

        permissions = await self._auth_repo.get_permissions(user=user)
        # # TODO: cache?
        # async def get_permissions(self, user: User) -> typing.Dict:
        #     user_groups = await self.get_groups(user)
        #     projects = await self._config_repo.get_projects_config()

        #     permissions = {
        #         'entities': {},
        #         'relations': {},
        #     }
        #     for project_name in projects:
        #         if project_name == '__all__':
        #             continue

        #         entity_types_config = await self._config_repo.get_entity_types_config(project_name)
        #         for etn, et in entity_types_config.items():
        #             # data
        #             if 'data' in et['config'] and 'permissions' in et['config']['data']:
        #                 for permission, groups in et['config']['data']['permissions'].items():
        #                     permissions['entities'][etn]['data']['permissions'] = {}
        #                     for group in groups:
        #                         if group in user_groups:
        #                             if etn not in permissions:
        #                                 permissions['entities'][etn] = {}
        #                             if 'data' not in permissions['entities'][etn]:
        #                                 permissions['entities'][etn]['data'] = {}
        #                             permissions['entities'][etn]['data'][permission] = []
        #                             if 'fields' in et['config']['data']:
        #                                 for field in et['config']['data']['fields'].values():
        #                                     if (
        #                                         'permissions' in field
        #                                         and permission in field['permissions']
        #                                         and group in field['permissions'][permission]
        #                                     ):
        #                                         permissions['entities'][etn]['data'][permission].append(field['system_name'])
        #     # TODO: check and use
        #     # TODO: relations
        #     print(permissions)
        #     return permissions
        return UserWithPermissions(**user.dict(), permissions=permissions)

    async def revoke_token(
        self,
        Authorize: AuthJWT,
    ):
        # TODO: expire both access and refresh token
        # TODO: use expiration time from token (created on the application server, so timestamp should be correct)
        raw_jwt = Authorize.get_raw_jwt()
        if raw_jwt['type'] == 'access':
            print('revoking access token')
            await self._auth_repo.denylist_add_token(raw_jwt['jti'], Authorize._access_token_expires.seconds)
        elif raw_jwt['type'] == 'refresh':
            print('revoking refresh token')
            await self._auth_repo.denylist_add_token(raw_jwt['jti'], Authorize._refresh_token_expires.seconds)
        else:
            raise Exception(f'Unkown token type: {raw_jwt["type"]}')
