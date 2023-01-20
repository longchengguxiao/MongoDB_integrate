# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/01/18
# @Author  : longchengguxiao
# @File    : MyMongodb.py
# @Version : 3.8.9 Python
# @Description : This class integrates the basic usage of MongoDB, including adding, checking, deleting and
# modifying, and outputting logs in the form of log. Please pay attention to the comments before use. Initialization
# needs to fill in host, port and other information

import logging
from typing import List, Dict, Union, Tuple, Sequence, Any, Mapping

import pymongo
from pymongo.collection import Collection
from pymongo.cursor import Cursor

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


class MyMongodb():
    def __init__(
            self,
            host: str = 'localhost',
            port: int = 27017,
            database: str = None,
            collection: str = None):
        """
        连接数据库查询指定文档，并作一些简单的增查删改操作。待编写整合的部分有：聚合(aggregate)，索引(createIndex)，
        :param host:
        :param port:
        :param dataset:
        :param collection:
        """
        self.client = pymongo.MongoClient(host=host, port=port)
        logging.info("Successfully connected the client on %s:%d", self.client.HOST, self.client.PORT)
        if not database or not collection:
            logging.error("Not find dataset name or collection name inputted")
            raise KeyError
        if database not in self.client.list_database_names():
            logging.warning("Database %s not exists in the client. \nBefore filled with data, it will not be saved in "
                            "the client.", database)
        self.db = self.client.get_database(database)
        logging.info("Successfully connected to dataset %s", database)
        if collection in self.db.list_collection_names():
            logging.warning("Collection %s not exists in the database. \nBefore filled with data, it will not be "
                            "saved in the database.", collection)
        self.collection: Collection = self.db.get_collection(collection)
        logging.info("Successfully connected to collection %s", collection)

    @staticmethod
    def remove_duplicates(ori_data: List) -> List:
        res = []
        for i in ori_data:
            if i not in res:
                res.append(i)
        return res

    def insert(self, insert_data: List[Dict], ordered: bool = False):
        """
        去除重复值后插入数据到收集表中\n
        :param insert_data:待插入数据以列表形式
        :param ordered: 顺序插入数据，出错时立即终止，默认为False
        """
        insert_data = self.remove_duplicates(insert_data)

        if len(insert_data) == 1:
            self.collection.insert_one(insert_data[0])
        else:
            self.collection.insert_many(insert_data, ordered=ordered)

        logging.info(
            f"Successfully inserted {insert_data} into collection {self.collection.name}")

    def find_data(self,
                  key: str,
                  value: str,
                  optional: str = None,
                  limit_num: int = 99999,
                  skip_num: int = 0) -> (Cursor,
                                               int):
        """
        查找以键值对或条件选择的所有值\n
        只查个数不返回结果可以用collection.estimated_document_count()\n
        带条件查询可以使用 collection.count_documents()\n
        :param key: 键名
        :param value: 对应的值,可选择的类型有\n
        '$regex', 匹配正则表达式, {'name': {'$regex': '^M.*'}}\n
        '$exists', 属性是否存在, {'name': {'exists': True}}\n
        '$type', 类型判断, {'age': {'type': 'int'}}\n
        :param optional: 可选择的条件，可选择的条件有\n
        '$lt', 大于, {'age': {'$lt': 20}}\n
        '$gt', 小于, {'age': {'$gt': 20}}\n
        '$lte', 大于等于, {'age': {'$lte': 20}}\n
        '$gte', 小于等于, {'age': {'$gte': 20}}\n
        '$ne', 不等于, {'age': {'$ne': 20}}\n
        '$in', 在范围内, {'age': {'in', [20, 23]}}\n
        '$nin', 不在范围内, {'age': {'nin', [20,23]}}\n
        :param limit_num 限制返回几个数据，默认值为99999即无限大
        :param skip_num 可选择跳过前几个数据，默认为0
        :return: 符合条件的所有数据并返回个数，是一个迭代器，可以用循环遍历
        """
        if not optional:
            results = self.collection.find({key: value}).limit(limit_num).skip(skip_num)
        else:
            results = self.collection.find(
                {key: {optional: value}}).limit(limit_num).skip(skip_num)
        return results, len(list(results))

    @staticmethod
    def sort_data(sorted_data: Cursor,
                  key: Union[str,
                             Sequence[Tuple[str,
                                            Union[int,
                                                  str,
                                                  Mapping[str,
                                                          Any]]]]],
                  rule: int = 1) -> Cursor:
        """
        输入排序字段返回排序结果，可以选择升降序\n
        :param sorted_data 待排序数据
        :param key: 排序字段,可以是字符串也可以是数组附带条件
        :param rule: 升降序规则，1为升序(pymongo.ASCENDING)，-1为降序(pymongo.DESCENDING)
        :return: 排序结果
        """
        sorted_data = sorted_data.clone()
        if isinstance(key, str):
            results = sorted_data.sort(key, rule)
        else:
            results = sorted_data.sort(key_or_list=key)
        return results

    def delete(self, filter: Dict, impact_data: str = 'all'):
        """
        在原表中删除过滤后的数据\n
        :param filter: 过滤器, 筛选出需要删除的数据
        :param impact_data: 影响的数据，默认为all，可选字段为single和all
        """
        results = None
        if impact_data == 'all':
            results = self.collection.delete_many(filter=filter)
        elif impact_data == 'single':
            results = self.collection.delete_one(filter=filter)
        else:
            logging.error("The field parameter 'impact_data' is incorrect, please reselect. Failed to delete")
        if results:
            logging.info("Successfully matched and deleted %d data.", results.deleted_count)

    def update(self, filter: Dict, update: Dict, impact_data: str = 'all'):
        """
        更新单个或多个数据的值\n
        :param filter: 过滤器，例如{'age': {'$gt': 20}}
        :param update: 更新后的值, 例如{'$set': {'age': 1}}, 操作符可使用$set(设置为某数), $inc(添加上某数)等
        :param impact_data: 影响的数据，默认为all，可选字段为single和all
        """
        results = None
        if impact_data == 'all':
            results = self.collection.update_many(filter=filter, update=update)
        elif impact_data == 'single':
            results = self.collection.update_one(filter=filter, update=update)
        else:
            logging.error("The field parameter 'impact_data' is incorrect, please reselect. Failed to update")
        if results:
            logging.info("Successfully matched %d data and updated %d data.", results.matched_count)




student1 = {
    'id': '20170102',
    'name': 'John',
    'age': 21,
    'gender': 'male'
}
student2 = {
    'id': '20170103',
    'name': 'Sally',
    'age': 19,
    'gender': 'female'
}
if __name__ == "__main__":
    mydb = MyMongodb(database='local', collection='students')
    mydb.insert([student1, student2])
    data, num = mydb.find_data(key='name', value='Jordan')
    mydb.sort_data(data, 'name')
