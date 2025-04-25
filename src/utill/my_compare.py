import math


def try_float(value) -> float:
    try:
        return float(value)
    except:
        return value


def same(a, b, float_precision=None) -> tuple[bool, float]:
    if a is None and b is None:
        return True, None

    if a is None or b is None:
        return False, None

    # Compare float
    a_float = try_float(a)
    b_float = try_float(b)
    if isinstance(a_float, float) or isinstance(b_float, float):
        try:
            if math.isnan(a_float) and math.isnan(b_float):
                return True, None

            if float_precision:
                a_float_rounded = round(a_float, float_precision)
                b_float_rounded = round(b_float, float_precision)

            return a_float_rounded == b_float_rounded, abs(a_float - b_float)
        except (ValueError, TypeError):
            raise Exception(f'Can\'t compare {a} to {b}')

    return str(a) == str(b), None
