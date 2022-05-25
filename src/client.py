import time
from traceback import format_exc
from typing import Dict

import redis
from loguru import logger as log

from src.models import ConsumerGroup, Consumer, StreamsMessage


class RedisStreamsClient(object):
    def __init__(self, redis_key: str, consumer_group: str = None, consumer_name: str = None,
                 host: str = 'localhost', port: int = 6379,
                 username: str = None, password: str = None,
                 max_len: int = 10000, ):
        self.redis_key = redis_key
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.max_len = max_len
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._connect_redis()
        if consumer_group and consumer_name:
            self._join_group()

    def _connect_redis(self):
        try:
            self.client = redis.Redis(
                host=self.host, port=self.port,
                username=self.username, password=self.password,
                retry_on_timeout=True)
            redis_version = self.client.info().get('redis_version')
            log.success(f'Connect redis {redis_version} server OK!')
        except Exception as e:
            log.error(f"Error connecting redis, detail: {e}, {format_exc}")

    def _join_group(self):
        if self.consumer_group not in self.list_groups():
            self.client.xgroup_create(self.redis_key, self.consumer_group, mkstream=True)
        self.client.xgroup_createconsumer(self.redis_key, self.consumer_group, self.consumer_name)
        log.success(f'Redis Streams {self.redis_key} joined group {self.consumer_group} '
                    f'as consumer={self.consumer_name} OK!')

    def get_steam_info(self):
        """获取当前 redis stream 的信息"""
        return self.client.xinfo_stream(self.redis_key)

    def list_groups(self) -> Dict[str, ConsumerGroup]:
        """获取当前 redis stream 中的消费者群组信息

        Returns: consumer groups in current streams

        """
        groups = dict()
        try:
            for group in self.client.xinfo_groups(self.redis_key):
                group['name'] = group['name'].decode("utf-8")
                group['last_delivered_id'] = group['last-delivered-id'].decode("utf-8")
                groups[group['name']] = ConsumerGroup(**group)
        except Exception as e:
            log.warning(f'Cannot list groups in streams={self.redis_key}, details: {e}')
        finally:
            return groups

    def list_group_consumers(self, consumer_group: str = None) -> Dict[str, Consumer]:
        """获取指定消费群组中的消费者信息

        Args:
            consumer_group: consumer group name

        Returns: consumers in group

        """
        consumer_group = self.consumer_group if not consumer_group else consumer_group
        consumers = {}
        for c in self.client.xinfo_consumers(self.redis_key, consumer_group):
            name = c['name'].decode("utf-8")
            consumer = Consumer(**{'name': name, 'pending': c['pending'], 'idle': c['idle']})
            consumers[name] = consumer
        return consumers

    def add_msg(self, data: dict):
        """向指定的 redis stream 中新增消息

        Args:
            data: message data(must be dict type)

        Returns: add result

        """
        try:
            if type(data) is not dict:
                raise TypeError(f'Redis stream data must be a dict, your data={data}')
            byte_data_id = self.client.xadd(
                name=self.redis_key, fields=data, id='*', maxlen=self.max_len)
            log.success(f'ADD message OK, ID={byte_data_id.decode("utf-8")}, data={data}')
            return True
        except Exception as e:
            log.error(f'Redis stream put data error, detail: {e}, {format_exc()}')
            return False

    def listen_msg(self, callback, block: int = 30000):
        """监听指定消费者群组的消息并回调函数

        Args:
            callback: 回调函数，用于处理stream消息
            block: 阻塞时长(ms)

        Returns: None

        """
        if not self.consumer_group or not self.consumer_name:
            log.error(f'To listen message, client must be initialized with group and consumer.')
            return
        while True:
            try:
                if response := self.client.xreadgroup(
                        groupname=self.consumer_group, consumername=self.consumer_name,
                        streams={self.redis_key: ">"}, block=block, count=1):
                    msg = self._parse_msg_response(response)
                    log.info(f'GET message OK, msg={msg.json()}')
                    callback(msg)
            except Exception as e:
                log.error(f'Redis stream listen message error, detail: {e}, {format_exc()}')
                time.sleep(10)

    def ack_msg(self, msg: StreamsMessage):
        """确认某条信息已消费

        Args:
            msg: 需要确认消费的消息

        Returns:

        """
        if not self.consumer_group or not self.consumer_name:
            log.error(f'To ack message, client must be initialized with group and consumer.')
            return
        self.client.xack(self.redis_key, self.consumer_group, msg.id)
        log.success(f'ACK message OK, msg={msg.json()}')

    @staticmethod
    def _parse_msg_response(response):
        if not response:
            return
        _, byte_info = response[0]
        byte_id, byte_data = byte_info[0]
        msg_id = byte_id.decode("utf-8")
        msg_data = {k.decode("utf-8"): v.decode("utf-8") for k, v in byte_data.items()}
        return StreamsMessage(**{'id': msg_id, 'data': msg_data})
