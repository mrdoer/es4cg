# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 14:54:20 2018

@author: eieuchh
"""
import pandas as pd
from elasticsearch import Elasticsearch

class VmElastic:
    def __init__(self):
        self.es = Elasticsearch()
        self.index_name = 'clean_vm'
        self.index_type = 'clean_vm_type'

#    def create_index(self,index_name="ott",index_type="ott_type"):
#        _index_mappings = {
#            "mappings": {
#                self.index_type: {
#                    "properties": {
#                        "label": {
#                            "type": "text",
#                            "index": True,
#                            "analyzer": "ik_max_word",
#                            "search_analyzer": "ik_max_word"
#                        },
#                        "verdict_message": {
#                            "type": "string",
#                            "index": "not_analyzed"
#                        }
#                    }
#                }
#
#            }
#        }
#        if self.es.indices.exists(index=self.index_name) is not True:
#            res = self.es.indices.create(index=self.index_name, body=_index_mappings, timeout=30)
#            print (res)

    def index_data_fromxlsx(self):
        file = pd.ExcelFile('verdict_message_cleaned_labeled.xlsx')
        df = pd.read_excel(file,'cleaned_vm')
        rows = df.shape[0]
        print(rows)
        labels = df['labels'].tolist()
        cleaned_vm = df['cleaned_vm'].tolist()
        
        for i in range(rows):
            doc = {}
            doc['label'] = labels[i]
            doc['vm'] = cleaned_vm[i]
            # doc['label'] = df.get_value(i,'labels')
            # doc['vm'] = df.get_value(i,'cleaned_vm')
            print('------------------------{}-------------------'.format(i))
            # print(doc)
            # print('-------------------------------------------')
            self.es.index(index = self.index_name, doc_type = self.index_type, id = i, body = doc)

ve = VmElastic()

ve.index_data_fromxlsx()
