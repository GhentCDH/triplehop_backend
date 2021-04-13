import re

RE_FIELD_CONVERSION = re.compile(r'(?<![$])[$][0-9]+')
RE_FIELD_DEF_CONVERSION = re.compile(r'(?<![$])[$]([a-z_]+)')
RE_FIELD_DEF_REL_ENT_CONVERSION = re.compile(r'(?<![$])[$]([a-z_]+)[-][>][$]([a-z]+)')


def dtu(string: str) -> str:
    '''Replace all dashes in a string with underscores.'''
    return string.replace('-', '_')


def utd(string: str) -> str:
    '''Replace all underscores in a string with dashes.'''
    return string.replace('_', '-')
