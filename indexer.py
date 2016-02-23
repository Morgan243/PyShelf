import threading
import time

from whoosh.index import create_in
import whoosh.index as index
from whoosh.qparser import QueryParser
from whoosh.fields import *
from whoosh.query import *
from whoosh.writing import BufferedWriter

import argparse

from queue import Queue
import queue
import os

from repute.net_core import NetCore, BroadcastCore
from repute.service_module import ServiceModule
from repute.peer_module import PeerModule

schemas = dict(
                web=dict(title=TEXT(stored=True),
                         url=ID(stored=True),
                         content=TEXT),

                def_schema=dict(title=TEXT(stored=True),
                                comment=TEXT(stored=True),
                                content=TEXT)
              )


class IndexerService(ServiceModule):
    base_service_name = 'INDEXER'

    def __init__(self, index_path='.index',
                 schema_name='default-schema',
                 new_schema=schemas['def_schema'],
                 listen_host=None,
                 listen_port=None):
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

        if index_path is not None and new_schema is not None:
            try:
                self.schema, self.__index = self.__create_index(index_path, new_schema)
            except:
                print("Couldn't create index at %s, trying to open it instead..." % index_path)
                self.__index = self.__open_index(index_path)
                print("Success!")

        elif index_path is not None and schema_name is not None:
            try:
                self.__index = self.__open_index(index_path)
            except:
                print("Couldn't open index at %s, trying to create it instead..." % index_path)
                self.schema, self.__index = self.__create_index(index_path, new_schema)
                print("Success!")
        else:
            msg = "Wtf?!\n%s\n%s" % (type(index_path), type(new_schema))
            raise ValueError(msg)

        self.schema_name = schema_name

        self.doc_queue = queue.Queue()

        self._searcher = self.__index.searcher()
        self.parser = None
        self.doc_count = 0
        self.idx_p_sec = 0.0
        self.print_index_stats()
        self.do_sync = False
        service_name = IndexerService.get_service_name(self.schema_name)


        ServiceModule.__init__(self, host=listen_host, port=listen_port,
                               service_name=service_name,
                               respond_to_bcasts=True)

    def print_index_stats(self):
        print("Doc count: %s " % str(self.__index.doc_count()))
        print("Doc count all: %s " % str(self.__index.doc_count_all()))
        print("Doc IDs: %s " % ", ".join(map(str, self.__index.reader().all_doc_ids())))
        print("Doc fields: %s " % ", ".join(map(str, self.__index.reader().all_stored_fields())))

    @staticmethod
    def get_service_name(schema_name='default-schema'):
        return 'indexer|%s' % schema_name

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


    def wk_run_doc_loader(self, verbose=True):
        self.doc_cnt = 0
        # keep loading while the service is up or there is work to do
        if verbose: print("Running doc loader...")
        #writer = ix.writer(procs=4, multisegment=True)

        w_args = dict(limitmb=256,
                      #procs=4,
                      #multisegment=True,
                      )
        #w_args = dict()

        with BufferedWriter(self.__index, period=120, limit=40, writerargs=w_args) as writer:
            while self.is_working or self.doc_queue.qsize():
                try:
                    start_t = time.time()
                    doc = self.doc_queue.get(timeout=5)
                    writer.add_document(**doc)
                    end_t = time.time()

                    self.doc_cnt += 1

                    self.idx_p_sec = 1.0/(end_t - start_t)
                    if verbose:
                        print("Indexed: %d (%.2f i/s) (pending: %d)" % (self.doc_cnt,
                                                                        self.idx_p_sec,
                                                                        self.doc_queue.qsize()))
                except queue.Empty as e:
                    time.sleep(.1)
                    pass

                if self.do_sync:
                    print("Committing")
                    writer.commit()
                    self.do_sync = False

        if verbose:
            print("Doc loader completing")

    def cb_add_document(self, **doc):
        self.doc_queue.put(doc)

    def cb_sync(self):
        self.do_sync = True

    def cb_query(self, q_str, field=None, raw_results=False):
        print("Query: %s" % q_str)
        if self.parser is None:
            field = 'content'
            self.parser = QueryParser(field, self.__index.schema)
            print(field)
            print(self.__index.schema)
            print(self.parser)

        if not self._searcher.up_to_date():
            self._searcher = self._searcher.refresh()

        q = self.parser.parse(q_str)

        results = self._searcher.search(q)

        if raw_results:
            return results
        else:
            return [str(r) for r in results]

    def cb_get_stats(self):
        self.print_index_stats()
        return dict(qsize=self.doc_queue.qsize(),
                    idxps=self.idx_p_sec,
                    dcnt=self.__index.doc_count())


def fetch_stats(host, port):
    stats = NetCore.exec_remote_callback(host, port,
                                         cb_func='get_stats',
                                         cb_kwargs={})
    return stats


def cli_query(query_func=None,
              query_args=None,
              query_kwargs=None,
              stats_func=None,
              sync_func=None):
    query_args = {} if query_args is None else query_args
    query_kwargs = {} if query_kwargs is None else query_kwargs

    while True:
        in_qry = input("Enter Query: ")
        if in_qry == '\q':
            break
        if in_qry == '\s' and stats_func:
            print(stats_func())
        elif in_qry == '\y' and sync_func:
            print("calling sync")
            sync_func()
            print("done")
        else:
            results = query_func(in_qry, *query_args, **query_kwargs)

            print("Found %d results" % len(results))
            print(("\n\t").join((str(r) for r in results)))


def recurse_load_files(path):
    #for root, subdirs, files in os.walk(path):
    #    print("root: %s" % str(root))
    #    print("\tsubdirs: %s" % str(subdirs))
    #    print("\tfiles: %s" % str(files))
    return [os.path.join(root, f) for root, _, files in os.walk(path) for f in files]

if __name__ == "__main__":
    ops = ['query', 'service']

    parser = argparse.ArgumentParser(description='Indexing Service')
    parser.add_argument("operation", metavar='O', type=str,
                        help=", ".join(ops))
    parser.add_argument('--index_files', metavar='I', type=str,
                        default=None,
                        help="Path to files to be indexed")
    parser.add_argument('--index_path', metavar='I', type=str,
                        default='.shelf_index',
                        help="Path to index directory to read on launch")

    parser.add_argument('--schema', type=str, default=None,
                        help="Schema name: " + ", ".join(schemas.keys()))

    parser.add_argument('--host', dest="service_host", type=str)
    parser.add_argument('--port', dest="service_port", type=int)
    parser.add_argument('--query', dest="query", type=str, default=None)

    parser.add_argument('--test', dest="run_test", default=0, type=int)

    args = parser.parse_args()


    #########
    ## Query Remote instance
    if args.operation == 'query':
        re_indexer = IndexerService.quick_connect()
        print("Connected!")

        cli_query(query_func=re_indexer.query,
                  stats_func=re_indexer.get_stats,
                  sync_func=re_indexer.sync)
        sys.exit(0)
    elif args.operation == 'service':
        schema = schemas.get(args.schema, None)
        schema = schemas['def_schema'] if schema is None else schema

        #########
        ## Start instance on this machine
        indexer = IndexerService(index_path=args.index_path,
                                 new_schema=schema,
                                 listen_host=args.service_host,
                                 listen_port=args.service_port)
        cli_query(query_func=indexer.cb_query, query_args=["content"])
    elif args.operation == 'index' and args.index_files is not None:
        #print(args.operation_args)
        #file_to_index = args.operation_args
        file_to_index = args.index_files
        if os.path.isdir(file_to_index):
            all_file_paths = recurse_load_files(file_to_index)
            choice = input("%d documents to index, continue? [Y/n]: " % len(all_file_paths))
            if choice != 'Y':
                print("Bailing out!")
                sys.exit(0)
        else:
            all_file_path = [file_to_index]
        re_indexer = IndexerService.quick_connect()
        print("Connected!")
        for fti in all_file_paths:
            doc = dict(title=fti,
                       comment='just_a test',
                       content=open(fti, 'r').read())
            print("Indexing: %s" % fti)
            re_indexer.add_document(**doc)
    else:
        print("Unknown operation: %s" % args.operation)
        print("Use one of: %s" % ", ".join(ops))
        sys.exit(1)

