from whoosh.fields import *
from .doc_parsers import *

name = 'notebook'
schema = dict(name=ID(stored=True),
              path=ID(stored=True,unique=True),
              dt=DATETIME(stored=True),
              notebook=STORED,                  # actual object
              code_cells=TEXT(stored=True),  # result of type()
              markdown_cells=TEXT(stored=True),   # human added/readable comment field
              )
q_fields = ['name', 'path', 'code_cells', 'markdown_cells']

#doc_parser = parse_file
#doc_parser = parse_python_obj_json
doc_parser = Parser(dt=date_parser, notebook=serial_parser)


description = dict(name=name, schema=schema,
                   q_fields=q_fields, parser=doc_parser)
