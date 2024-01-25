import os
import socket
import socketserver
import threading
from collections import deque
from enum import Enum

from bus import transcoding


class ComType(Enum):
    PUB = 1
    SUB = 2


if os.name == 'nt':
    class BrokerServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = True

        def __init__(self, server_address, RequestHandlerClass, mq_center):
            super().__init__(server_address, RequestHandlerClass)
            self.mq_center = mq_center
            self.mq_lock = threading.RLock()
            # self.mq_event = threading.Event()
            self.mq_event_table = {}
            self.com_type = 0

        # def server_activate(self):
        #     pass

        def server_bind(self):
            # TCP_NODELAY
            if self.allow_reuse_address and hasattr(socket, "SO_REUSEADDR"):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if self.allow_reuse_port and hasattr(socket, "SO_REUSEPORT"):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.TCP_NODELAY, 1)
            try:
                self.socket.bind(self.server_address)
            except OSError as e:
                print('server_bind', e)
                # os.unlink(self.server_address)
                # self.socket.bind(self.server_address)
            # self.server_address = self.socket.getsockname()

        def verify_request(self, request, client_address):
            """
            :param request: socket.socket fd=4, family=1, type=1, proto=0, laddr=/tmp/socket
            :param client_address: None
            :return:
            """

            # 首先先建立连接，获取通信类型
            msg = request.recv(1024)
            recv_msg = transcoding.bytes2json(msg)
            self.com_type = recv_msg.get('type')
            request.sendall(b'connect ok')

            return True
else:
    class BrokerServer(socketserver.ThreadingUnixStreamServer):

        def __init__(self, server_address: str | bytes, RequestHandlerClass, mq_center):
            super().__init__(server_address, RequestHandlerClass)
            self.mq_center = mq_center
            self.mq_lock = threading.RLock()
            # self.mq_event = threading.Event()
            self.mq_event_table = {}
            self.com_type = 0

        def server_bind(self):
            # TCP_NODELAY
            if self.allow_reuse_address and hasattr(socket, "SO_REUSEADDR"):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if self.allow_reuse_port and hasattr(socket, "SO_REUSEPORT"):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.TCP_NODELAY, 1)
            try:
                self.socket.bind(self.server_address)
            except OSError:
                os.unlink(self.server_address)
                self.socket.bind(self.server_address)
            self.server_address = self.socket.getsockname()

        def verify_request(self, request, client_address):
            """
            :param request: socket.socket fd=4, family=1, type=1, proto=0, laddr=/tmp/socket
            :param client_address: None
            :return:
            """

            # 首先先建立连接，获取通信类型
            msg = request.recv(1024)
            recv_msg = transcoding.bytes2json(msg)
            self.com_type = recv_msg.get('type')
            request.sendall(b'connect ok')

            return True


class BrokerRequestHandle(socketserver.BaseRequestHandler):

    # 处理客户端发布消息动作。消息生产者
    def pub_handle(self):
        while True:
            # self.server.mq_con.acquire()
            msg_item = self.request.recv(1024)
            if msg_item == b'':
                break
            # print(f'pub_handle({threading.current_thread().name}): {msg_item}')
            msg_item = transcoding.bytes2json(msg_item)
            msg = msg_item.get('msg')
            topic = msg_item.get('topic')
            self.server.mq_lock.acquire()
            mq_center = self.server.mq_center
            mq_event_table = self.server.mq_event_table
            # 将消息加入哈希链表
            mq_center.setdefault(topic, deque()).appendleft(msg)
            topic_event = mq_event_table.get(topic)
            if topic_event is None:
                topic_event = threading.Event()
                mq_event_table.update({topic: topic_event})
            # print(mq_center)
            self.server.mq_lock.release()
            topic_event.set()
            # self.server.mq_con.notify()
            self.request.send(b'publish ok')
            # self.server.mq_con.wait()
            # self.server.mq_con.release()

    # 处理客户端的订阅消息动作。消息消费者
    def sub_handle(self):
        msg_item = self.request.recv(1024)
        print(f'sub_handle: {msg_item}')
        if msg_item == b'':
            return
        msg_item = transcoding.bytes2json(msg_item)
        topic = msg_item.get('topic')
        while True:
            # self.server.mq_con.acquire()
            # print(f'sub_handle({threading.current_thread().name})')
            self.server.mq_lock.acquire()
            mq_center = self.server.mq_center
            msgs = mq_center.get(topic)
            self.server.mq_lock.release()
            if msgs is None or len(msgs) == 0:
                # print(f'{topic} is not exist. wait')
                print(self.client_address[0], self.client_address[1], topic, mq_center)
                # self.server.mq_con.wait()
                mq_event_table = self.server.mq_event_table
                topic_event = mq_event_table.get(topic)
                if topic_event is None:
                    mq_event_table.update({topic: threading.Event()})
                topic_event = self.server.mq_event_table.get(topic)
                topic_event.wait()
                topic_event.clear()
                # print(f'got sub msg')
            else:
                # for msg in msgs:
                #     self.request.sendall(transcoding.str2bytes(msg))
                target_msg = mq_center.get(topic).pop()
                try:
                    self.request.send(transcoding.json2bytes(target_msg))
                    # print(f'sub_handle send {target_msg}')
                except Exception as e:
                    print(e)
                    break
                # self.server.mq_con.notify()
            # self.server.mq_con.release()

    def handle(self):
        print(f'handle {self.request}')
        # self.server.mq_con.acquire()
        com_type = self.server.com_type
        if com_type == ComType.PUB.value:
            self.pub_handle()
        elif com_type == ComType.SUB.value:
            self.sub_handle()
        else:
            pass
        # self.server.mq_con.release()
        self.server.shutdown_request(self.request)
