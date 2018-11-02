# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 14:54:24 2018

@author: eieuchh
"""

#coding:utf8
import os
import time
from os import walk
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class ElasticObj:
    def __init__(self, index_name,index_type):
        self.index_name = index_name
        self.index_type = index_type
        
        _index_mappings = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "en_std": {
                            "type":      "standard",
                            "stopwords": "_english_"
                        }
                    }
                }
            },
            "mappings": {
                self.index_type: {
                    "properties": {
                        "label":"string",
                        "data_key": "string",
                        "title": "string",
                        "verict_message": {
                            "type": "text",
                            "index": True,
                            "analyzer": "en_std",
                            "search_analyzer":"en_std"
                            },
                        "description":{
                            "type": "text",
                            "index": True,
                            "analyzer":"en_std",
                            "search_analyzer":"en_std"
                            }
                    }
                }
            }
        }
        
        self.es = Elasticsearch()
        
        res = self.es.indices.create(index=self.index_name, body=_index_mappings)
        
    def index_xlsx(self):      
        cfi_xlsx = 'C:/Users/EIEUCHH/Desktop/CFI-data.xlsx'
        data_frame = pd.read_excel(pd.ExcelFile(cfi_xlsx))
        rows = data_frame.shape[0]
        df_list = []
        colums = data_frame.colums()
        
        for i in df.index:
            row = dict()
            for col in colums:
                row[col] = df.get_value(index=i, col = col)
       
            df_list.append(row)
        print('xlsx to list done! total: {} items'.format(len(df_list)))
        
        ACTIONS = list()
        i = 1 
        for item in df_list:
            action = {
                "_index": self.index_name,
                "_type": self.index_type,
                "_source": item
            }
            i = i + 1
            ACTIONS.append(action)
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=True)
        print('Performed %d actions' % success)       

    def bulk_Index_Data(self):
        '''
        用bulk将批量数据存储到es
        :return:
        '''
        list = [
            {"date": "2017-09-13",
             "source": "慧聪网",
             "link": "http://info.broadcast.hc360.com/2017/09/130859749974.shtml",
             "keyword": "电视",
             "title": "付费 电视 行业面临的转型和挑战"
             },
            {"date": "2017-09-13",
             "source": "中国文明网",
             "link": "http://www.wenming.cn/xj_pd/yw/201709/t20170913_4421323.shtml",
             "keyword": "电视",
             "title": "电视 专题片《巡视利剑》广获好评：铁腕反腐凝聚党心民心"
             },
            {"date": "2017-09-13",
             "source": "人民电视",
             "link": "http://tv.people.com.cn/BIG5/n1/2017/0913/c67816-29533981.html",
             "keyword": "电视",
             "title": "中国第21批赴刚果（金）维和部隊启程--人民 电视 --人民网"
             },
            {"date": "2017-09-13",
             "source": "站长之家",
             "link": "http://www.chinaz.com/news/2017/0913/804263.shtml",
             "keyword": "电视",
             "title": "电视 盒子 哪个牌子好？ 吐血奉献三大选购秘笈"
             }
        ]
        ACTIONS = []
        i = 1
        for line in list:
            action = {
                "_index": self.index_name,
                "_type": self.index_type,
                "_id": i, #_id 也可以默认生成，不赋值
                "_source": {
                    "date": line['date'],
                    "source": line['source'].decode('utf8'),
                    "link": line['link'],
                    "keyword": line['keyword'].decode('utf8'),
                    "title": line['title'].decode('utf8')}
            }
            i += 1
            ACTIONS.append(action)
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=True)
        print('Performed %d actions' % success)

es_obj = ElasticObj('cfi','cfi-type')
es_obj.index_xlsx()
