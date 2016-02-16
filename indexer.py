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
    def __init__(self, index_path='.index',
                 schema_name='default-schema',
                 new_schema=schemas['def_schema'],
                 listen_host=None,
                 listen_port=None):

        test = PeerModule.peer_from_service(self)
        print("DIR: %s" % str(dir(test)))
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
        service_name = IndexerService.get_service_name(self.schema_name)
        ServiceModule.__init__(self, host=listen_host, port=listen_port,
                               service_name=service_name,
                               respond_to_bcasts=True)

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


    def wk_run_doc_loader(self, verbose=False):
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
                    pass

        if verbose:
            print("Doc loader completing")

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass

    def cb_add_document(self, **doc):
        self.doc_queue.put(doc)

    def cb_query(self, q_str, field=None, raw_results=True):
        if self.parser is None:
            self.parser = QueryParser(field, self.__index.schema)

        if not self._searcher.up_to_date():
            self._searcher = self._searcher.refresh()

        q = self.parser.parse(q_str)

        results = self._searcher.search(q)

        if raw_results:
            return results
        else:
            return [str(r) for r in results]

    def cb_get_stats(self):
        return dict(qsize=self.doc_queue.qsize(),
                    idxps=self.idx_p_sec,
                    dcnt=self.doc_cnt)


def fetch_stats(host, port):
    stats = NetCore.exec_remote_callback(host, port,
                                         cb_func='get_stats',
                                         cb_kwargs={})
    return stats


def cli_query(indexer=None, query_func=None,
              query_args=[], query_kwargs={},
              stats_func=None, ):
    if query_func is None:
        query_func = indexer.query
        query_args = ["content"]
        query_kwargs = {}

    while True:
        in_qry = input("Enter Query: ")
        if in_qry == '\q': break

        if in_qry == '\s' and stats_func:
            print(stats_func())
        else:
            results = query_func(in_qry, *query_args, **query_kwargs)

            print("Found %d results" % len(results))
            print(("\n\t").join((str(r) for r in results)))

def remote_cli_query(host, port):
    print("Connecting")
    f = lambda q: NetCore.exec_remote_callback(host, port,
                                               cb_func='query',
                                               cb_kwargs=dict(q_str=q,
                                                              field='content',
                                                              raw_results=False))

    sf = lambda : fetch_stats(host, port)

    cli_query(query_func=f, stats_func=sf)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Indexing Service')
    parser.add_argument("operation", metavar='O', type=str,
                        help="service, query")
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

    ops = ['query', 'service']
    if args.operation not in ops:
        print("Unknown operation: %s" % args.operation)
        print("Use one of: %s" % ", ".join(ops))
        sys.exit(1)

    #########
    ## Query Remote instance
    if args.operation == 'query':
        port = args.service_port
        host = args.service_host
        if port is None:
            responses = BroadcastCore.bcast_and_recv_responses('yo dawwwwg')
            if len(responses) == 0:
                print("Found no indexers, exiting")
                sys.exit(0)

            print(responses)
            resp = responses[0]
            host = resp[1][0]
            port = int(resp[0].decode('utf-8').split(':')[-1])

        remote_cli_query(host, port)
        sys.exit(0)

    schema = schemas.get(args.schema, None)
    schema = schemas['def_schema'] if schema is None else schema

    #########
    ## Start instance on this machine
    indexer = IndexerService(index_path=args.index_path,
                             new_schema=schema,
                             listen_host=args.service_host,
                             listen_port=args.service_port)
    cli_query(query_func=indexer.cb_query, query_args=["content"])
