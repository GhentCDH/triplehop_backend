from pydantic import BaseModel, UUID4


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str = None


class User(BaseModel):
    id: UUID4
    username: str
    display_name: str = None
    disabled: bool = None


class UserInDB(User):
    hashed_password: str


class UserWithPermissions(User):
    permissions: dict
