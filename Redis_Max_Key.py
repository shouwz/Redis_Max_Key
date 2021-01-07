import os
import sys
import redis
import datetime
import time
import threading
from tqdm import tqdm


total_max_key = 100

class redis_max_key:

    def __init__(self, host, port):  # , password=123456):
        self.host = host
        self.port = port
        # self.password = password
        self.abc = []
        self.max_key_dict = {}

    def RedisScan(self):
        total_key = []
        # , password=self.password)
        client = redis.Redis(host=self.host, port=self.port)
        cursor = 0
        while True:
            cursor, keys = client.scan(cursor, match="*", count=200)
            for key in keys:
                total_key.append(key)
                print('{} : {}'.format(datetime.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S'), len(total_key)))
            if cursor == 0:
                break
            client.close()
        self.abc = total_key
        print('{} : 获取所有Key完成.\n'.format(
            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    def KeySizeDict(self):
        print('{} : 开始计算Key-Value大小.'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        # , password=self.password)
        client = redis.Redis(host=self.host, port=self.port)
        tqdm_key = tqdm(self.abc)
        for i in tqdm_key:
            key_size = client.memory_usage(i)
            if key_size is None:
                self.max_key_dict[i] = 0
            else:
                self.max_key_dict[i] = key_size
            tqdm_key.set_description("Processing")
        client.close()
        print('{} : Key-Value字典初始化完成.\n'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    def TopNkey(self):
        self.RedisScan()
        self.KeySizeDict()
        print('{} : 开始计算TOP{}的Key的大小.'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), total_max_key))
        lot = [(k, v) for k, v in self.max_key_dict.items()]
        nl = []
        if lot is None:
            pass
        else:
            while len(lot) > 0:
                nl.append(max(lot, key=lambda x: x[1]))
                lot.remove(nl[-1])
        return nl[0:total_max_key]


if __name__ == '__main__':
    redis_instance = redis_max_key(sys.argv[1], int(sys.argv[2]))
    top_max_key_length = redis_instance.TopNkey()
    for i in top_max_key_length:
        print('{} : {}{}'.format(i[0], round(i[1] / 1048576, 2), 'MB'))

