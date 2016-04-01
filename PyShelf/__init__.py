from .client import Client
from .client import PythonObjectClient as py_objects
from .client import NotebookClient as nb
from .index_descriptions import *

objects = py_objects()
notebooks = nb()
