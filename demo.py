import time
from datetime import datetime
from threading import Thread

from src.client import RedisStreamsClient

if __name__ == "__main__":
    client = RedisStreamsClient(redis_key='my-mq', consumer_group='my-cg', consumer_name='my-cn',
                                max_len=500, host='192.168.2.145')

    streams_info = client.get_steams_info()
    my_groups = client.list_groups()
    my_consumers = client.list_group_consumers()

    t = Thread(target=client.listen_msg, args=(client.ack_msg,))
    t.start()

    for i in range(10):
        test_msg = {'msg': 'hello', 'dt': str(datetime.now())}
        client.add_msg(test_msg)
        time.sleep(3)
