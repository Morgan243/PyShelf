import requests as r
import pprint

class Client(object):
    def __init__(self, description,
                 host="127.0.0.1", port=5000):
        self.name = description['name']
        self.parser = description['parser']
        self.schema = description['schema']
        self.uri = "http://%s:%d/%s" % (host, port, self.name)
        self.sync_uri = "http://%s:%d/__ix/sync" % (host, port)
        self.info_uri = "http://%s:%d/__ix/info" % (host, port)

    def index(self, **kwargs):
        # get initial dict and convert
        json_doc_dict = self.parser.doc_to_json(kwargs)
        for f, v in kwargs.items():
            if f not in self.schema.keys():
                #raise ValueError("%s not in schema" % f)
                print("WARNING: %s not in schema" %f)
            # Don't want to overwrite what the parser did
            elif f not in json_doc_dict:
                json_doc_dict[f] = v

        # put will have data in form
        resp = r.post(self.uri, json=json_doc_dict)
        return resp

    def query(self, q_str, **kwargs):
        # TODO be able to specify query fields through kwargs
        resp = r.get(self.uri, params={'q': q_str})
        doc = self.parser.json_to_doc(resp.json())
        return doc

    def sync(self):
        return r.get(self.sync_uri).json()

    def info(self):
        return r.get(self.info_uri).json()

if __name__ == "__main__":
    from index_descriptions.python_ix import description as podesc
    pyobj = Client(podesc)
    res = pyobj.query("users")
    pprint.pprint(res)



