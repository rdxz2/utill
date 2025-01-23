from .my_style import italic


def ask_yes_no(prompt: str = 'Continue?', yes_strings: tuple[str] = ('y', ), throw_if_no: bool = False) -> str:
    prompt = f'{prompt} ({yes_strings[0]}/no) : '
    yes = input(f'\n{italic(prompt)}') in yes_strings
    if not yes:
        if throw_if_no:
            raise Exception('Aborted by user')

    return yes
