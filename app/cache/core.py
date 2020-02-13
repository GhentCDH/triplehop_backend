def key_builder(func, self, *args, **kwargs):
    str_args = [str(arg) for arg in args]
    return f'{func.__module__}|{func.__name__}|{"|".join(str_args)}'
