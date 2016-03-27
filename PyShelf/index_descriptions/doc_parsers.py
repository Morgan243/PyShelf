import os
import datetime
import base64
import pickle
from dateutil import parser

# Protocol 2 is compatible with Python 2 and 3
pickle_protocol = 2

class Parser(object):
    def __init__(self, **kwargs):
        self.parser_map = kwargs

    @staticmethod
    def serialize(obj):
        ser = pickle.dumps(obj, pickle_protocol)
        enc = base64.b64encode(ser)
        return enc.decode()

    @staticmethod
    def deserialize(enc):
        ser = base64.b64decode(enc)
        obj = pickle.loads(ser)
        return obj

    @staticmethod
    def json_date_to_datetime(json_dt):
        return parser.parse(json_dt)

    @staticmethod
    def datetime_to_json(dt):
        return dt.isoformat()

    def json_to_doc(self, in_json):
        if "results" in in_json:
            ret_data = dict(in_json)
            ret_data['results'] = [self.json_to_doc(dict(r.items()))
                                   for r in in_json['results']]
            return ret_data

        ret_doc = dict()
        for f, value in in_json.items():
            if f in self.parser_map:
                #print("Running parser on %s" % f)
                ret_doc[f] = self.parser_map[f][0](value)
            else:
                ret_doc[f] = value
        return ret_doc

    def doc_to_json(self, in_doc):
        ret_dict = dict()
        for f, value in in_doc.items():
            if f in self.parser_map:
                ret_dict[f] = self.parser_map[f][1](value)
            elif isinstance(value, datetime.datetime):
                ret_dict[f] = Parser.datetime_to_json(value)
            else:
                ret_dict[f] = value

        return ret_dict
date_parser = (Parser.json_date_to_datetime, # Input to the service
               Parser.datetime_to_json)      # Output of the service

serial_parser = (Parser.deserialize, Parser.serialize)

