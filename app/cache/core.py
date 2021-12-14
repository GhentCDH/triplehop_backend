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


def create_schema_key_builder(func, self):
    return f'{func.__module__}|{func.__name__}|{self._project_name}|{self._user.id}'
