from starlette.requests import Request
from app.models.auth import UserWithPermissions


def no_arg_key_builder(func, *args):
    return f'{func.__module__}|{func.__name__}'


def skip_first_arg_key_builder(func, *args):
    str_args = [str(arg) for arg in args[1:]]
    return f'{func.__module__}|{func.__name__}|{"|".join(str_args)}'


def self_project_name_key_builder(func, self):
    return f'{func.__module__}|{func.__name__}|{self._project_name}'


def self_project_name_other_args_key_builder(func, self, *args):
    str_args = [str(arg) for arg in args]
    return f'{func.__module__}|{func.__name__}|{self._project_name}|{"|".join(str_args)}'


def request_user_key_builder(func, request: Request, user: UserWithPermissions):
    return f'{func.__module__}|{func.__name__}|{request.path_params["project_name"]}|{user.id}'
