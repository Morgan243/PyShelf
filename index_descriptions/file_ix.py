from whoosh.fields import *
from .doc_parsers import parse_file

name = 'file'
schema = dict(path=ID(stored=True, unique=True),
              size=NUMERIC(stored=True),
              mod_dt=DATETIME(stored=True),
              cretn_dt=DATETIME(stored=True),
              owner=ID(stored=True),
              group=ID(stored=True))
q_fields = ['path', 'size', 'owner', 'group']

doc_parser = parse_file

description = dict(name=name, schema=schema,
                   q_fields=q_fields, doc_parser=doc_parser)
