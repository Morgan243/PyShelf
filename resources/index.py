from flask.ext.restful import Resource, reqparse
from flask import Flask, request

parser = reqparse.RequestParser()
#parser.add_argument('item', type=dict)

class Index(Resource):
    def __init__(self, indexers):
        self.indexers = indexers
        self.get_reqparse = reqparse.RequestParser()
        self.get_reqparse.add_argument('q', type=str, required=False,
                                       help='No query string (q) provided')

        #self.reqparse.add_argument('description', type = str, default = "", location = 'json')
        super(Index, self).__init__()

    def get(self, index_name):
        args = self.get_reqparse.parse_args()

        #indexer = self.indexers.get(index_name, self.indexers['default'])
        try:
            indexer = self.indexers[index_name]
        except:
            return "No indexer for index %s" % index_name

        if args['q'] is not None:
            q_res = indexer.query(args['q'])
            result = dict(results=str(q_res))
        else:
            result = dict(_schema=indexer.schema_name,
                          fields=str(indexer.schema))
        return result

    def put(self, index_name):
        #print(dir(request))
        print("Index: " + index_name)
        post_doc = request.get_json()
        print("JSON: " + str(post_doc))
        self.indexers[index_name].add_document(**post_doc)
        return "k thanks"
        #args = parser.parse_args()
        #return args['item']
