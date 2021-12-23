import typing


def construct_def(type: str, type_name: str, props: typing.List) -> str:
    def_array = [f'{type} {type_name} {{']

    for prop in props:
        def_array.append(f'    {prop[0]}: {prop[1]}')

    def_array.append('}')

    return '\n'.join(def_array)
