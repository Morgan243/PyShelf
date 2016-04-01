from whoosh.fields import *
from .doc_parsers import *

name = 'file'
schema = dict(path=ID(stored=True, unique=True),
              name=TEXT(stored=True),
              size=NUMERIC(stored=True),
              mod_dt=DATETIME(stored=True),
              cretn_dt=DATETIME(stored=True),
              owner=ID(stored=True),
              group=ID(stored=True),
              text_content=TEXT(stored=True),
              binary_content=STORED())
q_fields = ['path', 'name', 'size', 'owner', 'group', 'text_content']

doc_parser = Parser(mod_dt=date_parser,
                    cretn_dt=date_parser)

description = dict(name=name, schema=schema,
                   q_fields=q_fields, parser=doc_parser)
