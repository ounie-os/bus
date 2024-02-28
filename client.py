import asyncio
import os
import socket
import struct
import threading
from json import JSONDecodeError
from queue import Queue

from bus import transcoding

MSG_HEAD_PACK_SIZE = 8
MSG_HEAD_DATA_SIZE = 4
MSG_HEAD_SIZE = MSG_HEAD_PACK_SIZE + MSG_HEAD_DATA_SIZE


class BusClient(threading.Thread):
    def __init__(self, addr, recv_queue: Queue):
        super().__init__()
        self.connect_flag = 0
        if os.name == 'nt':
            self.sk = socket.socket(family=socket.AF_INET)
        else:
            self.sk = socket.socket(family=socket.AF_UNIX)
        # self.sk.settimeout(3)
        self.remote_addr = addr
        self.queue = Queue()
        self.recv_queue = recv_queue

    def connect(self):
        try:
            ret = self.sk.connect(self.remote_addr)
            self.connect_flag = 1
        except Exception as e:
            print(e)

    def disconnect(self):
        self.connect_flag = 0

    def settimeout(self, value):
        self.sk.settimeout(value)

    def close(self):
        self.sk.close()


class BusClientAsync(threading.Thread):
    def __init__(self, addr, recv_queue: asyncio.Queue):
        super().__init__()
        self.connect_flag = 0
        if os.name == 'nt':
            self.sk = socket.socket(family=socket.AF_INET)
        else:
            self.sk = socket.socket(family=socket.AF_UNIX)
        # self.sk.settimeout(3)
        self.remote_addr = addr
        self.queue = Queue()
        self.recv_queue = recv_queue

    def connect(self):
        try:
            ret = self.sk.connect(self.remote_addr)
            self.connect_flag = 1
        except Exception as e:
            print(e)

    def disconnect(self):
        self.connect_flag = 0

    def settimeout(self, value):
        self.sk.settimeout(value)

    def close(self):
        self.sk.close()


class PubBusClient(BusClient):

    def __init__(self, addr, recv_queue: Queue = None):
        super().__init__(addr, recv_queue)

    def publish(self, topic, data):
        if self.connect_flag == 0:
            return
        msg = {'type': 1, 'topic': topic, 'msg': data}
        # print(f'publish {msg}')
        self.queue.put(transcoding.json2bytes(msg))

    def connect(self):
        try:
            ret = self.sk.connect(self.remote_addr)
            self.connect_flag = 1
            con_msg = {'type': 1}
            self.sk.sendall(transcoding.json2bytes(con_msg))
            connect_response = self.sk.recv(1024)
            print('PubBusClient:', connect_response)
        except Exception as e:
            print(e)

    def start_publish(self):
        self.connect()
        self.start()

    def run(self):
        while self.connect_flag:
            item = self.queue.get()
            try:
                self.sk.sendall(item)
            except BrokenPipeError:
                self.disconnect()
            try:
                recv_item = self.sk.recv(1024)
                if recv_item == b'':
                    self.disconnect()
                    break
                # broker的回复放入队列供读取
                # print(recv_item)
                if self.recv_queue is not None:
                    self.recv_queue.put(recv_item)
            except ConnectionResetError:
                self.disconnect()
                break
            except TimeoutError:
                print('pub timeout')
                pass

        self.close()


class SubBusClient(BusClient):

    def __init__(self, addr, recv_queue: Queue):
        super().__init__(addr, recv_queue)
        self.topic = ''

    def start_subscribe(self, topic):
        self.connect()
        msg = {'type': 2, 'topic': topic}
        self.topic = topic
        self.queue.put(transcoding.json2bytes(msg))
        self.start()

    def unsubcribe(self):
        self.disconnect()

    def connect(self):
        try:
            ret = self.sk.connect(self.remote_addr)
            print('SubBusClient connect', ret)
            self.connect_flag = 1
            con_msg = {'type': 2}
            self.sk.sendall(transcoding.json2bytes(con_msg))
            connect_response = self.sk.recv(1024)
            print('SubBusClient:', connect_response)
        except Exception as e:
            print('SubBusClient', e)

    def run(self):
        item = self.queue.get()
        self.sk.sendall(item)
        while self.connect_flag:
            try:
                recv_buffer = b''
                recv_item = self.sk.recv(1024)
                # 连接断开了
                if recv_item == b'':
                    self.disconnect()
                    print('subcribe broken')
                    break
                recv_buffer += recv_item
                while True:
                    recv_buffer_length = len(recv_buffer)
                    # 收到的数据包长度比包头都短
                    if recv_buffer_length < MSG_HEAD_SIZE:
                        break
                    # 找出包头
                    msg_header = recv_buffer[:MSG_HEAD_PACK_SIZE]
                    # 包头匹配
                    if msg_header == b'A5A55A5A':
                        msg_body_size = struct.unpack('I', recv_buffer[
                                                        MSG_HEAD_PACK_SIZE:MSG_HEAD_PACK_SIZE + MSG_HEAD_DATA_SIZE])[0]
                        # 收到的数据包长度小于要求的长度
                        if recv_buffer_length < MSG_HEAD_SIZE + msg_body_size:
                            # print(f'数据包长度小于包头加要求的长度{msg_body_size}')
                            break
                        the_item = recv_buffer[MSG_HEAD_SIZE: MSG_HEAD_SIZE + msg_body_size]
                        # broker的回复放入队列供读取
                        self.recv_queue.put(transcoding.bytes2json(the_item))
                        recv_buffer = recv_buffer[MSG_HEAD_SIZE + msg_body_size:]
                        # print(the_item)
                        # print(recv_buffer)
                        # print('------')

            except TimeoutError as e:
                print(e, self.topic)
            except JSONDecodeError as e:
                print(e, recv_item)
            except ConnectionAbortedError as e:
                print(f'topic:{self.topic} cancel subcribe: {e}')
                break

        self.close()


class SubBusClientAsync(BusClientAsync):

    def __init__(self, addr, recv_queue: asyncio.Queue):
        super().__init__(addr, recv_queue)
        self.topic = ''

    def start_subscribe(self, topic):
        self.connect()
        msg = {'type': 2, 'topic': topic}
        self.topic = topic
        self.queue.put(transcoding.json2bytes(msg))
        self.start()

    def unsubcribe(self):
        self.disconnect()

    def connect(self):
        try:
            ret = self.sk.connect(self.remote_addr)
            print('SubBusClient connect', ret)
            self.connect_flag = 1
            con_msg = {'type': 2}
            self.sk.sendall(transcoding.json2bytes(con_msg))
            connect_response = self.sk.recv(1024)
            print('SubBusClient:', connect_response)
        except Exception as e:
            print('SubBusClient', e)

    async def run_async(self):
        item = self.queue.get()
        self.sk.sendall(item)
        while self.connect_flag:
            try:
                recv_buffer = b''
                recv_item = self.sk.recv(1024)
                # 连接断开了
                if recv_item == b'':
                    self.disconnect()
                    print('subcribe broken')
                    break
                recv_buffer += recv_item
                while True:
                    recv_buffer_length = len(recv_buffer)
                    # 收到的数据包长度比包头都短
                    if recv_buffer_length < MSG_HEAD_SIZE:
                        break
                    # 找出包头
                    msg_header = recv_buffer[:MSG_HEAD_PACK_SIZE]
                    # 包头匹配
                    if msg_header == b'A5A55A5A':
                        msg_body_size = struct.unpack('I', recv_buffer[
                                                        MSG_HEAD_PACK_SIZE:MSG_HEAD_PACK_SIZE + MSG_HEAD_DATA_SIZE])[0]
                        # 收到的数据包长度小于要求的长度
                        if recv_buffer_length < MSG_HEAD_SIZE + msg_body_size:
                            # print(f'数据包长度小于包头加要求的长度{msg_body_size}')
                            break
                        the_item = recv_buffer[MSG_HEAD_SIZE: MSG_HEAD_SIZE + msg_body_size]
                        # broker的回复放入队列供读取
                        await self.recv_queue.put(transcoding.bytes2json(the_item))
                        recv_buffer = recv_buffer[MSG_HEAD_SIZE + msg_body_size:]
                        # print(the_item)
                        # print(recv_buffer)
                        # print('------')

            except TimeoutError as e:
                print(e, self.topic)
            except JSONDecodeError as e:
                print(e, recv_item)
            except ConnectionAbortedError as e:
                print(f'topic:{self.topic} cancel subcribe: {e}')
                break

        self.close()

    def run(self):
        asyncio.run(self.run_async())


if __name__ == '__main__':
    sub_client = SubBusClient('/tmp/socket', Queue())
    pub_client = PubBusClient('/tmp/socket', Queue())
    sub_client.connect()
    sub_client.start_subscribe('/user/new')
    pub_client.connect()
    pub_client.start_publish()
    pub_client.publish('/usr/new', '12345')
    # sub_client.subscribe('/user/new')
