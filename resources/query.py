from flask.ext.restful import Resource, reqparse
from flask import Flask, request

parser = reqparse.RequestParser()
#parser.add_argument('item', type=dict)

class Query(Resource):
    def __init__(self, indexer_service):
        self.indexer_service = indexer_service
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('q', type=str, required=False,
                                   help='No query string (q) provided',
                                   location='json')

        #self.reqparse.add_argument('description', type = str, default = "", location = 'json')
        super(Query, self).__init__()

    def get(self, query):
        args = self.reqparse.parse_args()
        return args['q']

    def put(self, query):
        #print(request)
        return "k thanks"
