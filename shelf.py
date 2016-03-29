__author__ = 'morgan'

from repute.net_core import NetCore, BroadcastCore
from repute.service_module import ServiceModule
from repute.peer_module import PeerModule

from indexer import IndexerService


class Shelf(PeerModule):
    def __init__(self):
        pass

    @staticmethod
    def connect():
        return IndexerService.quick_connect()

