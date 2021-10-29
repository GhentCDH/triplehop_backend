from pydantic import BaseModel, UUID4


class Token(BaseModel):
    access_token: str


class FormUser(BaseModel):
    username: str
    password: str


class User(BaseModel):
    id: UUID4
    username: str
    display_name: str = None
    disabled: bool = None


class UserInDB(User):
    hashed_password: str


class UserWithPermissions(User):
    # TODO: use typing.Dict for sub-type constraints
    permissions: dict


class UserWithPermissionsResponse(BaseModel):
    user: UserWithPermissions
