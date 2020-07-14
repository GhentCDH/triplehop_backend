from re import compile as re_compile

RE_FIELD_CONVERSION = re_compile(r'(?<![$])[$][0-9]+')
RE_FIELD_DEF_CONVERSION = re_compile(r'(?<![$])[$]([a-z_]+)')
RE_FIELD_DEF_REL_ENT_CONVERSION = re_compile(r'(?<![$])[$]([a-z_]+)[-][>][$]([a-z]+)')
RE_RECORD = re_compile('^[ev][_]([a-z0-9_]+)[^{]*({.*})$')


def dtu(string: str) -> str:
    '''Replace all dashes in a string with underscores.'''
    return string.replace('-', '_')


def utd(string: str) -> str:
    '''Replace all underscores in a string with dashes.'''
    return string.replace('_', '-')
