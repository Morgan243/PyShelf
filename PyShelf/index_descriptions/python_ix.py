from whoosh.fields import *
from .doc_parsers import *

name = 'python-obj'
schema = dict(name=ID(stored=True, unique=True),
              dt=DATETIME(stored=True),
              creator=ID(stored=True),
              obj=STORED,                  # actual object
              obj_type=TEXT(stored=True),  # result of type()
              comment=TEXT(stored=True),   # human added/readable comment field
              contextual=TEXT(stored=True) # meta info on the object
              )
q_fields = ['name', 'creator', 'obj_type', 'comment', 'contextual']

#doc_parser = parse_file
#doc_parser = parse_python_obj_json
doc_parser = Parser(dt=date_parser, obj=serial_parser)


description = dict(name=name, schema=schema,
                   q_fields=q_fields, parser=doc_parser)
