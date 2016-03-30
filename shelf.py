__author__ = 'morgan'
import logging

from indexer import Indexer

from index_descriptions.file_ix import description as file_desc

from flask import Flask
from flask.ext.restful import Api
from resources.info import Info
from resources.index import Index
from resources.admin import Sync

logging.basicConfig(level=logging.DEBUG)

def auto_build_indexers(descriptions):
    ixers = dict()
    for d in descriptions:
        ix = Indexer(index_path='.shelf_%s_index' % d['name'],
                     new_schema=d['schema'],
                     doc_parser=d['doc_parser'],
                     q_fields=d['q_fields'])
        ixers[d['name']] = ix

    return ixers


def create_app():
    logging.info("Building indexers")
    indexers = auto_build_indexers([file_desc])
    resource_kwargs = dict(indexers=indexers)

    app = Flask(__name__)
    api = Api(app)

    api.add_resource(Info, '/__ix/info',
                     resource_class_kwargs=resource_kwargs)
    api.add_resource(Index, '/<string:index_name>',
                     resource_class_kwargs=resource_kwargs)
    api.add_resource(Sync, '/__ix/sync',
                     resource_class_kwargs=resource_kwargs)

    @app.before_first_request
    def launch_indexers():
        for name, ixer in indexers.items():
            print("Launching %s" % name)
            ixer.launch_doc_loader()

    return app, api

if __name__ == '__main__':
    app, api = create_app()
    app.run(debug=True)
