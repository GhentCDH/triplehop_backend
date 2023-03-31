import asyncpg

from app.models.auth import User


def no_arg_key_builder(func, _):
    return f"{func.__module__}|{func.__name__}"


def skip_self_key_builder(func, self, *args, **kwargs):
    str_args = [str(arg) for arg in args]
    str_kwargs = [f"{k}__{str(v)}" for k, v in kwargs.items()]
    return f'{func.__module__}|{func.__name__}|{"|".join(str_args + str_kwargs)}'


def skip_self_connection_key_builder(func, self, *args, **kwargs):
    str_args = [
        str(arg) for arg in args if not isinstance(arg, asyncpg.connection.Connection)
    ]
    str_kwargs = [f"{k}__{str(v)}" for k, v in kwargs.items() if k != "connection"]
    return f'{func.__module__}|{func.__name__}|{"|".join(str_args + str_kwargs)}'


def self_project_name_key_builder(func, self):
    return f"{func.__module__}|{func.__name__}|{self._project_name}"


def self_project_name_entity_type_name_key_builder(func, self):
    return f"{func.__module__}|{func.__name__}|{self._project_name}|{self._entity_type_name}"


def self_project_name_other_args_key_builder(func, self, *args, **kwargs):
    str_args = [str(arg) for arg in args]
    str_kwargs = [f"{k}__{str(v)}" for k, v in kwargs.items()]
    return f'{func.__module__}|{func.__name__}|{self._project_name}|{"|".join(str_args + str_kwargs)}'


def get_permissions_key_builder(func, self, user: User):
    return f"{func.__module__}|{func.__name__}|{user.id}"


def create_schema_key_builder(func, self):
    return f"{func.__module__}|{func.__name__}|{self._project_name}|{self._user.id}"
