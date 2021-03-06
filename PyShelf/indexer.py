import threading
import time

from whoosh.index import create_in
import whoosh.index as index
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.fields import *
from whoosh.query import *
from whoosh.writing import BufferedWriter

import argparse
import logging

import queue
import os

## TODO:
## - How does Whoosh support multiple indices
## - Posting to index

schemas = dict(
                def_schema=dict(title=TEXT(stored=True),
                                comment=TEXT(stored=True),
                                content=TEXT),
                web=dict(title=TEXT(stored=True),
                         url=ID(stored=True, unique=True),
                         content=TEXT),
                file=dict(path=ID(stored=True, unique=True),
                          size=NUMERIC(stored=True),
                          mod_dt=DATETIME(stored=True),
                          cretn_dt=DATETIME(stored=True),
                          owner=ID(stored=True),
                          group=ID(stored=True))
              )

class Indexer(object):
    def __init__(self, index_path='.index',
                 schema_name='default-schema',
                 new_schema=schemas['def_schema'],
                 q_fields=None,
                 listen_host=None,
                 listen_port=None,
                 doc_parser=None,
                 launch_doc_loader=False):
        #TODO Use index.exists_in to check index dir
        # use storage.index_exists() to check for inde??

        if new_schema is not None and index_path is None:
            msg = "Mush provide path for new index"
            raise ValueError(msg)

        if index_path is None and new_schema is None:
            msg = "Must provide at index path and/or scheam (see options)"
            raise ValueError(msg)

        if index_path is not None and new_schema is None:
            self.__index = self.__open_index(index_path)
            self.schema = self.__index.schema

        if index_path is not None and new_schema is not None:
            try:
                self.schema, self.__index = self.__create_index(index_path, new_schema)
            except:
                #print("Couldn't create index at %s, trying to open it instead..." % index_path)
                logging.info("Couldn't create index at %s, trying to open it instead..." % index_path)
                self.__index = self.__open_index(index_path)
                self.schema = self.__index.schema
                logging.info("Success!")

        elif index_path is not None and schema_name is not None:
            try:
                self.__index = self.__open_index(index_path)
                self.schema = self.__index.schema
            except:
                logging.info("Couldn't open index at %s, trying to create it instead..." % index_path)
                self.schema, self.__index = self.__create_index(index_path, new_schema)
                logging.info("Success!")
        else:
            msg = "Wtf?!\n%s\n%s" % (type(index_path), type(new_schema))
            raise ValueError(msg)

        self.schema_name = schema_name

        self.doc_queue = queue.Queue()
        if q_fields is None:
            type_ok = lambda f: isinstance(f, ID) or isinstance(f, TEXT)
            print("Building q fields")
            self.q_fields = [name for name, f in new_schema.items() if type_ok(f) ]
        else:
            self.q_fields = q_fields
        print("Q FIELDS in CTOR: %s" % str(self.q_fields))
        self._searcher = self.__index.searcher()
        self._buffered_searcher = None
        self.parser = None
        self.doc_count = self.__index.doc_count()
        self.idx_p_sec = 0.0
        self.do_sync = False
        self.doc_parser = doc_parser
        self.doc_loader_thread = None
        self.is_working = False
        if launch_doc_loader:
            self.launch_doc_loader()

    def print_index_stats(self):
        print("Doc count: %s " % str(self.__index.doc_count()))
        print("Doc count all: %s " % str(self.__index.doc_count_all()))
        print("Doc IDs: %s " % ", ".join(map(str, self.__index.reader().all_doc_ids())))
        print("Doc fields: %s " % ", ".join(map(str, self.__index.reader().all_stored_fields())))

    @staticmethod
    def __open_index(index_path):
        if os.path.isfile(index_path) or not os.path.isdir(index_path):
            msg = "Can't open index at %s" % index_path
            raise ValueError(msg)
        return index.open_dir(index_path)

    @staticmethod
    def __create_index(index_path, schema_dict):
        if os.path.isfile(index_path) or os.path.isdir(index_path):
            msg = "Can't create new index at %s, file/dir exists there" % index_path
            raise ValueError(msg)

        os.makedirs(index_path)
        tmp_schema = Schema(**schema_dict)
        tmp_index = create_in(index_path, tmp_schema)
        return tmp_schema, tmp_index

    def launch_doc_loader(self):
        if self.doc_loader_thread is None:
            self.is_working = True
            self.doc_loader_thread = threading.Thread(target=self.run_doc_loader,
                                                      name='doc-loader',
                                                      daemon=True)
            self.doc_loader_thread.start()

    def run_doc_loader(self, verbose=True):
        self.doc_cnt = 0
        # keep loading while the service is up or there is work to do
        if verbose:
            logging.debug("Running doc loader...")

        w_args = dict(limitmb=128,
                      #procs=4,
                      #multisegment=True,
                      )
        #w_args = dict()

        with BufferedWriter(self.__index, period=1, limit=15, writerargs=w_args) as writer:
            # This searcher is weird...
            self._buffered_searcher = writer.searcher()
            while self.is_working or self.doc_queue.qsize():
                try:
                    start_t = time.time()
                    doc = self.doc_queue.get(timeout=1)
                    if self.doc_parser is not None:
                        #p_doc = self.doc_parser.json_to_doc(doc)
                        p_doc = doc
                    else:
                        p_doc = doc

                    #writer.add_document(**p_doc)

                    # Will not delete existing documents if they haven't been committed
                    writer.update_document(**p_doc)
                    end_t = time.time()

                    self.doc_cnt += 1

                    self.idx_p_sec = 1.0/(end_t - start_t)
                    if verbose:
                        logging.debug("Indexed: %d (%.2f i/s) (pending: %d)" % (self.doc_cnt,
                                                                                self.idx_p_sec,
                                                                                self.doc_queue.qsize()))
                except queue.Empty as e:
                    time.sleep(.1)

                if self.do_sync:
                    logging.debug("%s committing" % self.schema_name)
                    writer.commit()
                    self.do_sync = False

        if verbose:
            logging.debug("Doc loader completing")

    def add_document(self, **doc):
        self.doc_queue.put(doc)

    def sync(self):
        self.do_sync = True

    def get_searcher(self):
        # TODO: Is this a concurrency problem?
        return self.__index.searcher()

    def query(self, q_str, field=None, raw_results=False, serializer=None):
        logging.debug("Query: %s" % q_str)
        if self.parser is None:
            logging.info("Query parser on: %s" % str(self.q_fields))
            self.parser = MultifieldParser(self.q_fields, self.__index.schema)

        q = self.parser.parse(q_str)
        print(q)

        # get reader closed error
        #with self.__index.searcher() as searcher:
        #    results = searcher.search(q)

        # TODO: Which searcher should I take?!?!
        if True:
            #searcher = self.__index.searcher()
            searcher = self.get_searcher()
        elif self._buffered_searcher is not None:
            print("Using buffered searcher")
            searcher = self._buffered_searcher
        elif not self._searcher.up_to_date():
            print("refreshing searcher")
            self._searcher = self._searcher.refresh()
            searcher = self._searcher

        results = searcher.search(q)

        if raw_results:
            if serializer is not None:
                return serializer(results)
            else:
                return results
        else:
            if serializer is not None:
                return [serializer(r) for r in results]
            else:
                return [r for r in results]


    def get_stats(self):
        return dict(queue_size=self.doc_queue.qsize(),
                    docs_per_sec=self.idx_p_sec,
                    doc_cnt=self.__index.doc_count(),
                    doc_loader_running=self.is_working)


if __name__ == "__main__":
    pass

    #parser = argparse.ArgumentParser(description='Indexing Service')
    #parser.add_argument('--index_files', metavar='I', type=str,
    #                    default=None,
    #                    help="Path to files to be indexed")
    #parser.add_argument('--index_path', metavar='I', type=str,
    #                    default='.shelf_index',
    #                    help="Path to index directory to read on launch")

    #parser.add_argument('--schema', type=str, default=None,
    #                    help="Schema name: " + ", ".join(schemas.keys()))

    #parser.add_argument('--host', dest="service_host", type=str)
    #parser.add_argument('--port', dest="service_port", type=int)
    #parser.add_argument('--query', dest="query", type=str, default=None)

    #parser.add_argument('--test', dest="run_test", default=0, type=int)

    #args = parser.parse_args()
    #schema = schemas.get(args.schema, None)
    #schema = schemas['def_schema'] if schema is None else schema

