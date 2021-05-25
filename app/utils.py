import re

RE_FIELD_CONVERSION = re.compile(
    # zero, one or multiple (inverse) relations
    r'(?:[$]ri?_[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}->)*'
    # zero or one (inverse) relations; dot for relation property and arrow for entity property
    r'(?:[$]ri?_[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}(?:[.]|->)){0,1}'
    # one property (entity or relation)
    r'[.]?[$](?:[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}|id|display_name)'
)


def dtu(string: str) -> str:
    '''Replace all dashes in a string with underscores.'''
    return string.replace('-', '_')


def utd(string: str) -> str:
    '''Replace all underscores in a string with dashes.'''
    return string.replace('_', '-')
