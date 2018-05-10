import collections
from deeputil import Dummy

from operator import attrgetter

DUMMY = Dummy()


def memoize(f):
    # from: https://goo.gl/aXt4Qy
    class memodict(dict):
        __slots__ = ()

        def __missing__(self, key):
            self[key] = ret = f(key)
            return ret
    return memodict().__getitem__


@memoize
def load_object(imp_path):
    """Given a path (python import path), load the object.

    eg of path: logagg.formatters.nginx_access
              : logagg.forwarders.mongodb
    """
    module_name, obj_name = imp_path.split('.', 1)
    module = __import__(module_name)
    obj = attrgetter(obj_name)(module)

    return obj


import traceback


def log_exception(self, __fn__):
    self.log.exception('error_during_run_Continuing', fn=__fn__.func_name,
                       tb=repr(traceback.format_exc()))


from threading import Thread


def start_daemon_thread(target, args=()):
    """starts a deamon thread for a given target function and arguments."""
    th = Thread(target=target, args=args)
    th.daemon = True
    th.start()
    return th


def serialize_dict_keys(d, prefix=""):
    """returns all the keys in a dictionary.

    >>> serialize_dict_keys({"a": {"b": {"c": 1, "b": 2} } })
    ['a', 'a.b', 'a.b.c', 'a.b.b']
    """
    keys = []
    for k, v in d.iteritems():
        fqk = '%s%s' % (prefix, k)
        keys.append(fqk)
        if isinstance(v, dict):
            keys.extend(serialize_dict_keys(v, prefix="%s." % fqk))

    return keys

class MarkValue(str): pass

def flatten_dict(d, parent_key='', sep='.',
                    ignore_under_prefixed=True, mark_value=True):
    '''
    >>> flatten_dict({"a": {"b": {"c": 1, "b": 2, "__d": 'ignore', "_e": "mark"} } })
    {'a.b.b': 2, 'a.b.c': 1, 'a.b._e': "'mark'"}
    '''
    items = {}
    for k in d:
        if ignore_under_prefixed and k.startswith('__'):
            continue
        v = d[k]
        if mark_value and k.startswith('_') and not k.startswith('__'):
            v = MarkValue(repr(v))

        new_key = sep.join((parent_key, k)) if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.update(flatten_dict(v, new_key, sep=sep,
                                        ignore_under_prefixed=True,
                                        mark_value=True)
                            )
        else:
            items[new_key] = v
    return items


import numbers


def is_number(x): return isinstance(x, numbers.Number)


from re import match


spaces = (' ', '\t', '\n')
def ispartial(x):
    '''
    If log line starts with a space it is recognized as a partial line
    >>> ispartial('<time> <event> <some_log_line>')
    False
    >>> ispartial(' <space> <traceback:> <some_line>')
    True
    >>> ispartial('         <tab> <traceback:> <some_line>')
    True
    >>> ispartial('   <white_space> <traceback:> <some_line>')
    True
    '''
    if x[0] in spaces:
        return True
    else:
        return False
