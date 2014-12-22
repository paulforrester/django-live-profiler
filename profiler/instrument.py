from datetime import datetime

from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.sql.constants import MULTI
from django.conf import settings

DEFAULT_SQL_COMPILERS = ['django.db.models.sql.compiler.SQLCompiler',]
SQL_COMPILERS = getattr(settings, 'LIVE_PROFILE_SQL_COMPILERS', DEFAULT_SQL_COMPILERS)

from aggregate.client import get_client

from profiler import _get_current_view


# Prior to 1.6, django did not have a general import function.  This is taken from the django 1.7 branch
from importlib import import_module
from django.utils import six
import sys

def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError:
        msg = "%s doesn't look like a module path" % dotted_path
        six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        msg = 'Module "%s" does not define a "%s" attribute/class' % (
            dotted_path, class_name)
        six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])



def execute_sql(self, *args, **kwargs):
    client = get_client()
    if client is None:
        return self.__execute_sql(*args, **kwargs)
    try:
        q, params = self.as_sql()
        if not q:
            raise EmptyResultSet
    except EmptyResultSet:
        if kwargs.get('result_type', MULTI) == MULTI:
            return iter([])
        else:
            return
    start = datetime.now()
    try:
        return self.__execute_sql(*args, **kwargs)
    finally:
        d = (datetime.now() - start)
        client.insert({'query' : q, 'view' : _get_current_view(), 'type' : 'sql'},
                      {'time' : 0.0 + d.seconds * 1000 + d.microseconds/1000, 'count' : 1})

INSTRUMENTED = False

if not INSTRUMENTED:
    for compiler_class_name in SQL_COMPILERS:
        compiler_class = import_string(compiler_class_name)
        compiler_class.__execute_sql = compiler_class.execute_sql
        compiler_class.execute_sql = execute_sql
    INSTRUMENTED = True

