from pydantic import BaseModel, UUID4


class Tokens(BaseModel):
    access_token: str
    refresh_token: str = None


class FormUser(BaseModel):
    username: str
    password: str


class User(BaseModel):
    id: UUID4
    username: str
    display_name: str = None
    disabled: bool = None


class UserWithHashedPassword(User):
    hashed_password: str


class UserWithPermissions(User):
    # TODO: use typing.Dict for sub-type constraints
    permissions: dict


class UserWithPermissionsResponse(BaseModel):
    user: UserWithPermissions
