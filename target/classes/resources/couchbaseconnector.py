#!/usr/bin/python
# -*- coding: utf-8 -*-

import storm
from couchbase import Couchbase
import json, os, uuid, logging



class CouchBaseBolt(storm.BasicBolt):
    global couchbase
    couchbase = Couchbase('172.26.6.9:8091', username='Administrator', password='WbY7fn4WBCGcUPH')
    global bucket
    bucket = couchbase['default']

    def process(self, tup):
        handler = logging.FileHandler("/Users/apolion/logfile.txt", "w", encoding = "UTF-8")

        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

        try:
            key = str(uuid.uuid4())
            myjson = json.loads(tup.values[0])
            bucket["TEST_SPOUT_%s"%(key)] = json.dumps(myjson, sort_keys=True)
            root_logger.info(myjson)
            
        except Exception as inst:
            root_logger.info("EXCEPTION!")
            root_logger.info(myjson)
            root_logger.info(inst)
            root_logger.info(inst.args)
        


CouchBaseBolt().run()