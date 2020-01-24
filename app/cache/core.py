def key_builder(func, self, *args, **kwargs):
    print(f'{func.__module__}|{func.__name__}|{"|".join(args)}')
    return f'{func.__module__}|{func.__name__}|{"|".join(args)}'
