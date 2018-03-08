
from deeputil import deepgetattr
from deeputil import Dummy, AttrDict
DUMMY_LOGGER = Dummy()


from operator import attrgetter

def load_object(imp_path):
    '''
    Given a path (python import path), load the object.

    eg of path: logagg.formatters.nginx_access
              : logagg.forwarders.mongodb
    '''
    module_name, obj_name = imp_path.split('.', 1)
    module = __import__(module_name)
    obj = attrgetter(obj_name)(module)

    return obj


import traceback

def log_exception(self, __fn__):
    self.log.exception('Error during run Continuing ...' , fn=__fn__.func_name,
                        tb=repr(traceback.format_exc()))


from threading import Thread

def start_daemon_thread(target, args=()):
    th = Thread(target=target, args=args)
    th.daemon = True
    th.start()
    return th

def serialize_dict_keys(d, prefix=""):
    """ returns all the keys in a dictionary
    >>> serialize_dict_keys({"a": {"b": {"c": 1, "b": 2} } })
    ['a', 'a.b', 'a.b.c', 'a.b.b']
    """

    keys = []
    for k,v in d.iteritems():
        fqk = '%s%s' % (prefix, k)
        keys.append(fqk)
        if isinstance(v, dict):
            keys.extend(serialize_dict_keys(v, prefix="%s." % fqk))

    return keys

def flatten_dict(d):
    '''
    >>> flatten_dict({"a": {"b": {"c": 1, "b": 2} } })
    {'a.b.b': 2, 'a.b.c': 1}
    '''
    fd = dict()
    keys = serialize_dict_keys(d)
    d = AttrDict(d)
    for key in keys:
        value = deepgetattr(d, key)
        if isinstance(value, dict):
            pass
        else:
            fd[key] = value
    return fd

import numbers

def is_number(x): return isinstance(x, numbers.Number)
