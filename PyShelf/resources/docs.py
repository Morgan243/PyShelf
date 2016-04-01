from flask.ext.restful import Resource, reqparse
from flask import Flask, request

parser = reqparse.RequestParser()

class Docs(Resource):
    def __init__(self, indexers, parsers):
        self.indexers = indexers
        self.parsers = parsers
        self.get_reqparse = reqparse.RequestParser()
        self.get_reqparse.add_argument('limit', type=int, required=False,
                                       help='Max num docs to retrieve')

        #self.reqparse.add_argument('description', type = str, default = "", location = 'json')
        super(Docs, self).__init__()

    def get(self, index_name):
        args = self.get_reqparse.parse_args()

        #indexer = self.indexers.get(index_name, self.indexers['default'])
        try:
            indexer = self.indexers[index_name]
        except:
            return "No indexer for index %s" % index_name

        parser = self.parsers[index_name]
        docs = [parser.doc_to_json(d) for d in indexer.get_searcher().documents()]
        return docs

