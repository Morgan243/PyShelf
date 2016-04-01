__author__ = 'morgan'
import logging

from indexer import Indexer

from index_descriptions.file_ix import description as file_desc
from index_descriptions.python_ix import description as python_desc
from index_descriptions.notebook_ix import description as notebook_desc

from flask import Flask
from flask.ext.restful import Api
from resources.info import Info
from resources.index import Index
from resources.admin import Sync
from resources.docs import Docs
import os
import argparse

logging.basicConfig(level=logging.DEBUG)

def auto_build_indexers(descriptions, indexing_dir='./'):
    ixers = dict()
    for d in descriptions:
        p = os.path.join(indexing_dir, 'shelf_%s_index' % d['name'])
        ix = Indexer(index_path=p,
                     new_schema=d['schema'],
                     schema_name=d['name'],
                     doc_parser=d['parser'],
                     q_fields=d['q_fields'])
        ixers[d['name']] = ix

    return ixers


def create_app(indexing_dir='./'):
    logging.info("Building indexers")
    desc_to_serve = [file_desc, python_desc, notebook_desc]
    indexers = auto_build_indexers(desc_to_serve, indexing_dir=indexing_dir)
    parsers = {d['name']: d['parser'] for d in desc_to_serve}
    resource_kwargs = dict(indexers=indexers)
    index_res_kwargs = dict(indexers=indexers, parsers=parsers)

    app = Flask(__name__)
    api = Api(app)

    api.add_resource(Info, '/__ix/info',
                     resource_class_kwargs=resource_kwargs)
    api.add_resource(Sync, '/__ix/sync',
                     resource_class_kwargs=resource_kwargs)
    api.add_resource(Index, '/<string:index_name>',
                     resource_class_kwargs=index_res_kwargs)
    api.add_resource(Docs, '/<string:index_name>/docs',
                     resource_class_kwargs=index_res_kwargs)
    @app.before_first_request
    def launch_indexers():
        for name, ixer in indexers.items():
            print("Launching %s" % name)
            ixer.launch_doc_loader()

    return app, api

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-p', '--index-path', dest='indexing_dir',
                        type=str, default='./')

    args = parser.parse_args()

    app, api = create_app(indexing_dir=args.indexing_dir)
    app.run(debug=True)
