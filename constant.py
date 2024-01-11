import os

if os.name == 'posix':
    broker_server_addr = '/tmp/socket'
elif os.name == 'nt':
    broker_server_addr = ('127.0.0.1', 6666)
