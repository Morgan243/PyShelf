import os
from dateutil import parser

def parse_file(in_json):
    #if not os.path.isfile(in_json['path']):
    ret_data = dict(in_json)
    print("Parsing input: %s" % str(ret_data))
    ret_data['mod_dt'] = parser.parse(ret_data['mod_dt'])
    ret_data['cretn_dt'] = parser.parse(ret_data['cretn_dt'])
    return ret_data

