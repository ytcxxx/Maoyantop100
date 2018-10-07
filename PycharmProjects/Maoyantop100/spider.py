#!/User/anaconda3/python3
# -*- coding: utf-8 -*-
# @Time     :   2018/10/7/007 8:28
# @Author   :   Qingyunke
import json
import time
from pymongo import MongoClient
import requests
from requests.exceptions import RequestException
import re
from multiprocessing import Pool
from config import *


def get_one_page(url):
    try:
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36'}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None


def parse_one_page(html):
    pattern = re.compile('<dd>.*?board-index.*?>(\d+)</i>.*?data-src="(.*?)".*?name"><a.*?>(.*?)</a>.*?star">(.*?)</p>'
                         + '.*?releasetime">(.*?)</p>.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?</dd>', re.S)
    items = re.finditer(pattern, html)
    for item in items:
        yield {
            'ranking': item.group(1),
            'image': item.group(2),
            'title': item.group(3),
            'actor': item.group(4).strip()[3:],
            'time': item.group(5).strip()[5:],
            'score': item.group(6) + item.group(7),
        }
    # items = re.findall(pattern, html)
    # for item in items:
    #     yield{
    #         'ranking': item[0],
    #         'image': item[1],
    #         'title': item[2],
    #         'actor': item[3].strip()[3:],
    #         'time': item[4].strip()[5:],
    #         'score': item[5] + item[6],
    #     }


def write_to_file(content):
    with open('movies.txt', 'a', encoding='utf-8') as f:
        f.write(json.dumps(content, ensure_ascii=False) + '\n')
        f.close()


def store_in_mongodb(content):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    collection.update_many({'title': content.get('title')}, {'$set': content}, upsert=True)


def main(offset):
    url = 'http://maoyan.com/board/4?offset=' + str(offset)
    html = get_one_page(url)
    for item in parse_one_page(html):
        print(item)
        write_to_file(item)
        store_in_mongodb(item)


if __name__ == '__main__':
    pool = Pool(9)
    pool.map(main, [i * 10 for i in range(10)])
    pool.close()
    pool.join()
    # for i in range(10):
    #     main(offset=i * 10)
    #     time.sleep(0.3)
