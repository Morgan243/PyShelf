from flask.ext.restful import Resource

class Info(Resource):
    def __init__(self, indexers):
        self.indexers = indexers

    def fields_to_json(self, fields):
        return {fn: str(ft) for fn, ft in fields.items()}

    def make_info(self):
        return {name: dict(fields=self.fields_to_json(ixer.schema._fields),
                           stats=ixer.get_stats())
                for name, ixer in self.indexers.items()}

    def get(self):
        info = self.make_info()
        return info

    def put(self):
        pass
