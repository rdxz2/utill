class Styles:
    NONE = '\033[0m'
    ITALIC = '\033[3m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def make_style(styles_or_colors: list[Styles | Colors] | Styles | Colors, string: str) -> str:
    if type(styles_or_colors) == list:
        return ''.join(styles_or_colors) + string + Styles.NONE
    else:
        return styles_or_colors + string + Styles.NONE


def bold(string: str) -> str:
    return make_style(Styles.BOLD, string)


def italic(string: str) -> str:
    return make_style(Styles.ITALIC, string)


def underline(string: str) -> str:
    return make_style(Styles.UNDERLINE, string)


def color(string: str, color: Colors) -> str:
    return make_style(color, string)
