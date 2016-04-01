from flask.ext.restful import Resource, reqparse
from flask import Flask, request

#from

parser = reqparse.RequestParser()
#parser.add_argument('item', type=dict)

class Index(Resource):
    def __init__(self, indexers, parsers):
        self.indexers = indexers
        self.parsers = parsers
        self.get_reqparse = reqparse.RequestParser()
        self.get_reqparse.add_argument('q', type=str, required=False,
                                       help='No query string (q) provided')

        super(Index, self).__init__()

    def get(self, index_name):
        args = self.get_reqparse.parse_args()

        try:
            indexer = self.indexers[index_name]
        except:
            return "No indexer for index %s" % index_name

        if args['q'] is not None:
            parser = self.parsers[index_name]
            q_res = indexer.query(args['q'], raw_results=True,
                                  serializer=None)

            print([dict(r.items()) for r in q_res])

            q_results = [parser.doc_to_json(dict(r.items())) for r in q_res]
            result = dict(results=q_results,
                          runtime=q_res.runtime)
        else:
            result = dict(_schema=indexer.schema_name,
                          fields=str(indexer.schema))
        return result

    def post(self, index_name):
        ## Just calls put
        return self.put(index_name)

    def put(self, index_name):
        #print(dir(request))
        #print("Index: " + index_name)
        #print("form: " + str(request.form))
        #print("data: " + str(request.data))
        post_doc = request.get_json()
        #print("JSON: " + str(post_doc))
        if post_doc is not None:
            doc = self.parsers[index_name].json_to_doc(post_doc)
            self.indexers[index_name].add_document(**doc)
        return "k thanks"
        #args = parser.parse_args()
        #return args['item']
