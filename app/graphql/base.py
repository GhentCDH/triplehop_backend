from typing import List


def construct_type_def(type_name: str, props: List) -> str:
    type_def_array = [f'type {type_name} {{']

    for prop in props:
        type_def_array.append(f'    {prop[0]}: {prop[1]}')

    type_def_array.append('}')

    return '\n'.join(type_def_array)
