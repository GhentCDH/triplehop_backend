def key_builder(func, self, *args, **kwargs):
    return f'{func.__module__}|{func.__name__}|{"|".join(args)}'
