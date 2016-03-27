import argparse
import os
import datetime
import json
import pandas as pd
from PyShelf.index_descriptions import object_shelf
from PyShelf.index_descriptions import notebook_shelf
import requests as r
import pprint
import uuid

os.environ['NO_PROXY'] = '127.0.0.1'

# TODO: Detect being run in notebook, and do fancy displays with HTML or other

class Client(object):
    def __init__(self, description,
                 host="127.0.0.1", port=5000):
        self.name = description['name']
        self.parser = description['parser']
        self.schema = description['schema']
        self.uri = "http://%s:%d/%s" % (host, port, self.name)
        self.sync_uri = "http://%s:%d/__ix/sync" % (host, port)
        self.info_uri = "http://%s:%d/__ix/info" % (host, port)
        self.docs_uri = "%s/docs" % self.uri

    #@staticmethod
    def res_to_dataframe(self, results):
        return pd.DataFrame(results)

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

    def query(self, q_str, as_dataframe=True, **kwargs):
        # TODO be able to specify query fields through kwargs
        resp = r.get(self.uri, params={'q': q_str})
        doc = self.parser.json_to_doc(resp.json())
        if as_dataframe:
            res = doc['results']
            if len(res) == 0:
                return pd.DataFrame(columns = list(self.schema.keys()))
            else:
                return self.res_to_dataframe(doc['results'])#pd.DataFrame(doc['results'])
        else:
            return doc

    def sync(self):
        return r.get(self.sync_uri).json()

    def info(self, as_dataframe=True):
        ret_data = r.get(self.info_uri).json()
        if as_dataframe:
            raw_data = [dict(index_name=idx_name, fields=list(info['fields'].keys()), **info['stats'])
                        for idx_name, info in ret_data.items()]
            return pd.DataFrame(raw_data).set_index('index_name')

    def docs(self, as_dataframe=True):
        j_objs = r.get(self.docs_uri).json()
        ret_data = [self.parser.json_to_doc(j) for j in j_objs]
        if as_dataframe:
            return self.res_to_dataframe(ret_data)#pd.DataFrame(ret_data)
        else:
            return ret_data

class PythonObjectClient(Client):
    def __init__(self):
        super(PythonObjectClient, self).__init__(description=object_shelf)

    def index(self, obj, name=None, creator='unknown',
              obj_type=None, comment='', dt=None):
        """
        Add an arbitrary (must be pickle-able) python object to the python object
        index. Good place to persist dictionaries, DataFrames, etc.

        Arguments
        ---------
        obj : object
            Python object that can be pickled
        name : str (default=None)
            Unique Id for this object, if None, one will be generated
        """

        if isinstance(obj, pd.DataFrame):
            contextual = str(obj.columns)
        elif isinstance(obj, dict):
            contextual = str(list(obj.keys()))
        elif isinstance(obj, pd.Series):
            contextual = "%s: %s" % (obj.name, str(obj.index))
        elif isinstance(obj, str):
            contextual = str(obj)
        else:
            contextual=''

        if name is None:
            name = str(uuid.uuid1())

        if dt is None:
            dt = datetime.datetime.now()
        return super(PythonObjectClient, self).index(obj=obj,
                                                     name=name,
                                                     creator=creator,
                                                     obj_type=str(type(obj)),
                                                     comment=comment,
                                                     contextual=contextual,
                                                     dt=dt)
class NotebookClient(Client):
    def __init__(self):
        super(NotebookClient, self).__init__(description=notebook_shelf)

    @staticmethod
    def file_is_notebook(path):
        name = os.path.split(path)[-1]
        if '.ipynb' in name:
            return True
        else:
            return False

    def index(self, name=None, path=None, notebook=None, dt=None, code_cells=None, markdown_cells=None):

        """
        Index an IPython (Jupyter) botebook file - Parsers out code and markdwon source cells
        so that they are searchable

        Arguments
        ---------
        """
        if path is None and notebook is None:
            msg = "Must specify notebook data or path"
            raise ValueError(msg)

        if notebook is None and path is not None:
            # Full path is more helpful
            path = os.path.realpath(path)
            notebook = json.load(open(path, 'r'))
            name = os.path.split(path)[-1]
            dt = datetime.datetime.fromtimestamp(os.path.getctime(path))

        code_cells = "\n".join(["\n".join(c['source']) for c in notebook['cells'] if c['cell_type'] == 'code'])
        markdown_cells = "\n".join(["\n".join(c['source']) for c in notebook['cells'] if c['cell_type'] == 'markdown'])

        if name is None:
            name = str(uuid.uuid1())

        if dt is None:
            dt = datetime.datetime.now()
        return super(NotebookClient, self).index(name=name,
                                                 path=path,
                                                 notebook=notebook,
                                                 dt=dt,
                                                 code_cells=code_cells,
                                                 markdown_cells=markdown_cells)

    def res_to_dataframe(self, results):
        return pd.DataFrame(results).set_index('name')


    # TODO: generalize by making a to_dataframe method on the derived class
#    def docs(self, as_dataframe=True):
#        ret = super(NotebookClient, self).docs(as_dataframe)
#        if as_dataframe:
#            return ret.set_index('name')
#        else:
#            return ret
#
#    def query(self, q_str, as_dataframe=True, **kwargs):
#        ret = super(NotebookClient, self).query(q_str, as_dataframe, **kwargs)
#        if as_dataframe:
#            return ret.set_index('name')
#        else:
#            return ret

if __name__ == "__main__":
    #from index_descriptions.python_ix import description as podesc
    #pyobj = Client(podesc)
    #res = pyobj.query("users")
    #pprint.pprint(res)
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--index-notebooks', dest='indexing_dir',
                        type=str, default=None)

    args = parser.parse_args()

    # Quick hack to index a bunch of notebooks
    if args.indexing_dir is not None:
        in_p = args.indexing_dir

        is_nb = NotebookClient.file_is_notebook
        if os.path.isdir(in_p):
            print("is directory")
            nb_client = NotebookClient()
            notebooks = [os.path.join(root, f) for root, _, files in os.walk(in_p) for f in files if is_nb(f)]
            #print(notebooks)
            print("Num notebooks: %s" % len(notebooks))

            for i, nb in enumerate(notebooks[:]):
                print("[%d/%d]Indexing: %s" % (i, len(notebooks), nb))
                nb_client.index(path=nb)




