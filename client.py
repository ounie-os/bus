import socket
import threading
from queue import Queue

import transcoding


class BusClient(threading.Thread):
    def __init__(self, addr, recv_queue: Queue):
        super().__init__()
        self.connect_flag = 0
        self.sk = socket.socket(family=socket.AF_UNIX)
        self.sk.settimeout(3)
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

    def __init__(self, addr, recv_queue: Queue):
        super().__init__(addr, recv_queue)

    def publish(self, topic, data):
        if self.connect_flag == 0:
            return
        msg = {'type': 1, 'topic': topic, 'msg': data}
        print(f'publish {msg}')
        self.queue.put(transcoding.json2bytes(msg))

    def connect(self):
        try:
            ret = self.sk.connect(self.remote_addr)
            self.connect_flag = 1
            con_msg = {'type': 1}
            self.sk.sendall(transcoding.json2bytes(con_msg))
            self.sk.recv(1024)
        except Exception as e:
            print(e)

    def start_publish(self):
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
                print(recv_item)
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

    def subscribe(self, topic):
        msg = {'type': 2, 'topic': topic}
        self.queue.put(transcoding.json2bytes(msg))
        self.start()

    def unsubcribe(self):
        self.disconnect()

    def connect(self):
        try:
            ret = self.sk.connect(self.remote_addr)
            self.connect_flag = 1
            con_msg = {'type': 2}
            self.sk.sendall(transcoding.json2bytes(con_msg))
            self.sk.recv(1024)
        except Exception as e:
            print(e)

    def run(self):
        item = self.queue.get()
        self.sk.sendall(item)
        while self.connect_flag:
            try:
                recv_item = self.sk.recv(1024)
                # 连接断开了
                if recv_item == b'':
                    self.disconnect()
                    print('subcribe broken')
                    break
                print(recv_item)
                # broker的回复放入队列供读取
                self.recv_queue.put(recv_item)
            except TimeoutError:
                pass

        self.close()


if __name__ == '__main__':
    sub_client = SubBusClient('/tmp/socket', Queue())
    pub_client = PubBusClient('/tmp/socket', Queue())
    sub_client.connect()
    sub_client.subscribe('/user/new')
    pub_client.connect()
    pub_client.start_publish()
    pub_client.publish('/usr/new', '12345')
    # sub_client.subscribe('/user/new')