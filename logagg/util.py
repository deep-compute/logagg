
from deeputil import Dummy
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

