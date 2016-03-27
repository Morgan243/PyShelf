from .client import Client
from .client import PythonObjectClient as py_objects
from .client import NotebookClient as nb
from .index_descriptions import *

objects = py_objects()
notebooks = nb()

# docs and info are same for all, so just use any here
info = objects.info
docs = objects.docs
