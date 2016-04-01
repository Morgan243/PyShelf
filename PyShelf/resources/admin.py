from flask.ext.restful import Resource, reqparse

class Admin(Resource):
    def __init__(self, indexers):
        self.indexers = indexers

    def get(self):
        pass

    def put(self):
        pass

class Sync(Resource):
    def __init__(self, indexers):
        self.indexers = indexers
        self.get_reqparse = reqparse.RequestParser()
        self.get_reqparse.add_argument('ix', type=str, required=False,
                                       help='Index to sync')

    def get(self):
        args = self.get_reqparse.parse_args()
        if args['ix'] is not None:
            self.indexers[args['ix']].sync()
            ret = dict(synced=[args['ix']])
        else:
            for name, ixer in self.indexers.items():
                ixer.sync()
            ret = dict(synced=list(self.indexers.keys()))
        return ret
