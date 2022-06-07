import aiocache
import fastapi
import starlette

from fastapi.exceptions import HTTPException
from fastapi_jwt_auth.auth_jwt import AuthJWT
from fastapi_jwt_auth.exceptions import JWTDecodeError, MissingTokenError
from passlib.context import CryptContext

from app.cache.core import get_permissions_key_builder
from app.db.auth import AuthRepository
from app.db.core import get_repository_from_request
from app.mgmt.config import ConfigManager
from app.models.auth import User, UserWithPermissions


class AuthManager:
    def __init__(
        self,
        request: starlette.requests.Request,
    ):
        self._auth_repo = get_repository_from_request(request, AuthRepository)
        self._config_manager = ConfigManager(request)

    async def authenticate_user(
        self,
        username: str,
        password: str,
    ):
        user = await self._auth_repo.get_user_with_hashed_password(username=username.lower())

        if user is None:
            raise HTTPException(status_code=400, detail='Incorrect username or password')

        pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')
        if not pwd_context.verify(password, user.hashed_password):
            raise HTTPException(status_code=400, detail='Incorrect username or password')

        return user

    # TODO: clear cache if user permissions are modified
    @aiocache.cached(key_builder=get_permissions_key_builder)
    async def _get_permissions(
        self,
        user: User,
    ):
        user_groups = await self._auth_repo.get_groups(user)
        projects = await self._config_manager.get_projects_config()

        permissions = {}
        for project_name in projects:
            if project_name == '__all__':
                continue

            for er, config in {
                'entities': await self._config_manager.get_entity_types_config(project_name),
                'relations': await self._config_manager.get_relation_types_config(project_name),
            }.items():
                for tn, tc in config.items():
                    # data
                    if 'data' in tc['config'] and 'permissions' in tc['config']['data']:
                        for permission, groups in tc['config']['data']['permissions'].items():
                            for group in groups:
                                if group not in user_groups:
                                    continue

                                # Entity permissions
                                if project_name not in permissions:
                                    permissions[project_name] = {}
                                if er not in permissions[project_name]:
                                    permissions[project_name][er] = {}
                                if tn not in permissions[project_name][er]:
                                    permissions[project_name][er][tn] = {}
                                if 'data' not in permissions[project_name][er][tn]:
                                    permissions[project_name][er][tn]['data'] = {}
                                permissions[project_name][er][tn]['data'][permission] = []

                                # Field permissions
                                if 'fields' not in tc['config']['data']:
                                    continue

                                for field in tc['config']['data']['fields'].values():
                                    if (
                                        'permissions' in field
                                        and permission in field['permissions']
                                        and group in field['permissions'][permission]
                                    ):
                                        permissions[project_name][er][tn]['data'][permission].append(
                                            field['system_name']
                                        )

                    # es_data
                    if 'es_data' in tc['config'] and 'permissions' in tc['config']['es_data']:
                        for permission, groups in tc['config']['es_data']['permissions'].items():
                            for group in groups:
                                if group not in user_groups:
                                    continue

                                # Entity permissions
                                if project_name not in permissions:
                                    permissions[project_name] = {}
                                if er not in permissions[project_name]:
                                    permissions[project_name][er] = {}
                                if tn not in permissions[project_name][er]:
                                    permissions[project_name][er][tn] = {}
                                if 'es_data' not in permissions[project_name][er][tn]:
                                    permissions[project_name][er][tn]['es_data'] = {}
                                permissions[project_name][er][tn]['es_data'][permission] = []

        return permissions

    async def get_current_active_user_with_permissions(
        self,
        Authorize: AuthJWT,
    ):
        user = None

        # Validate user credentials
        # If token is missing, load anonymous user
        # Anonymous users can have permissions
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

        return UserWithPermissions(
            **user.dict(),
            permissions=await self._get_permissions(user)
        )

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


async def get_current_active_user_with_permissions(
    request: starlette.requests.Request,
    Authorize: AuthJWT = fastapi.Depends(),
) -> UserWithPermissions:
    auth_manager = AuthManager(request)
    return await auth_manager.get_current_active_user_with_permissions(Authorize)


def allowed_entities_or_relations_and_properties(
    user: UserWithPermissions,
    project_name: str,
    entities_or_relations: str,
    section: str,
    permission: str,
):
    if project_name not in user.permissions:
        return {}

    if entities_or_relations not in user.permissions[project_name]:
        return {}

    allowed = {
        tn: perms[section][permission]
        for tn, perms in user.permissions[project_name][entities_or_relations].items()
        if section in perms and permission in perms[section]
    }

    if permission == 'get':
        return {
            tn: ['id', *props]
            for tn, props in allowed.items()
        }
    return allowed
