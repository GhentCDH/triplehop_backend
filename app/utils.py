import re

BATCH_SIZE = 500

RE_FIELD_CONVERSION = re.compile(
    # zero, one or multiple (inverse) relations
    r"(?:[$]ri?_[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}->)*"
    # zero or one (inverse) relations; dot for relation property and arrow for entity property
    r"(?:[$]ri?_[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}(?:[.]|->)){0,1}"
    # one property (entity or relation)
    r"[.]?[$](?:[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}|id|display_name)"
)

RE_SOURCE_PROP_INDEX = re.compile(
    # uuid followed by a number between square brackets
    r"^(?P<property>[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12})\[(?P<index>[0-9]*)\]$"
)


def dtu(string: str) -> str:
    """Replace all dashes in a string with underscores."""
    return string.replace("-", "_")


def utd(string: str) -> str:
    """Replace all underscores in a string with dashes."""
    return string.replace("_", "-")


def relation_label(relation_type_id: str) -> str:
    """Construct a relation_label from a relation type id."""
    # Special case '_source_'
    if relation_type_id == "_source_":
        return "_source_"
    return f"e_{dtu(relation_type_id)}"


def first_cap(input: str) -> str:
    """Capitalize the first letter of a text string."""
    return input[0].upper() + input[1:]


def nested_key_exists(dictionary, *keys):
    """Check if a nested key exists in a dictionary."""
    for key in keys:
        if key in dictionary:
            dictionary = dictionary[key]
        else:
            return False
    return True
