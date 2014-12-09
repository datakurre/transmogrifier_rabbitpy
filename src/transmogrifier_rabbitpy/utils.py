# -*- coding: utf-8 -*-


def to_boolean_when_looks_boolean(value):
    if value is not None:
        if value.lower() in ['on', 'yes', 'true']:
            return True
        elif value.lower() in ['off', 'no', 'false']:
            return False
    return value
