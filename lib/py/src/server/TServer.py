# -*- encoding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import Queue
import os
import threading

import logging

logger = logging.getLogger(__name__)

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport


"""
transport = TSocket.TServerSocket(port=9090) # 负责连接
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
"""


class TServer:
    """Base interface for a server, which must have a serve() method.

    Three constructors for all servers:
    1) (processor, serverTransport)
    2) (processor, serverTransport, transportFactory, protocolFactory)
    3) (processor, serverTransport,
        inputTransportFactory, outputTransportFactory,
        inputProtocolFactory, outputProtocolFactory)
    """

    def __init__(self, *args):
        # 弄清楚 processor/server_transport, factory1, factory2, protocol1, protocol2)之间的关系
        # TServer解决了初始化参数的问题
        #
        if (len(args) == 2):
            self.__initArgs__(args[0], args[1],
                              TTransport.TTransportFactoryBase(),
                              TTransport.TTransportFactoryBase(),
                              TBinaryProtocol.TBinaryProtocolFactory(),
                              TBinaryProtocol.TBinaryProtocolFactory())
        elif (len(args) == 4):
            self.__initArgs__(args[0], args[1], args[2], args[2], args[3], args[3])
        elif (len(args) == 6):
            self.__initArgs__(args[0], args[1], args[2], args[3], args[4], args[5])

    def __initArgs__(self, processor, serverTransport,
                     inputTransportFactory, outputTransportFactory,
                     inputProtocolFactory, outputProtocolFactory):
        self.processor = processor
        self.serverTransport = serverTransport
        self.inputTransportFactory = inputTransportFactory
        self.outputTransportFactory = outputTransportFactory
        self.inputProtocolFactory = inputProtocolFactory
        self.outputProtocolFactory = outputProtocolFactory

    def serve(self):
        pass


# 用于测试(一次只处理一个连接)
class TSimpleServer(TServer):
    """
    Simple single-threaded server that just pumps around one transport.
    """

    def __init__(self, *args):
        TServer.__init__(self, *args)

    def serve(self):
        # 开始监听
        self.serverTransport.listen()
        # 准备接受请求
        while True:
            # TSocket.TSocket
            client = self.serverTransport.accept()
            if not client:
                continue

            # 接收到请求之后，获取输入、输出
            # 为 Socket提供额外的功能
            itrans = self.inputTransportFactory.getTransport(client)
            otrans = self.outputTransportFactory.getTransport(client)

            # 添加协议控制
            iprot = self.inputProtocolFactory.getProtocol(itrans)
            oprot = self.outputProtocolFactory.getProtocol(otrans)

            # 然后一次只服务一个对象?
            try:
                while True:
                    # 调用Processor来处理数据
                    self.processor.process(iprot, oprot)
            except TTransport.TTransportException as tx:
                pass
            except Exception as x:
                logger.exception(x)

            itrans.close()
            otrans.close()


# 通过Thread来处理一个连接
class TThreadedServer(TServer):
    """Threaded server that spawns a new thread per each connection."""

    def __init__(self, *args, **kwargs):
        TServer.__init__(self, *args)
        self.daemon = kwargs.get("daemon", False)

    def serve(self):
        self.serverTransport.listen()
        while True:
            try:
                client = self.serverTransport.accept()
                if not client:
                    continue
                # 通过Thread来处理一个连接(传统做法)
                t = threading.Thread(target=self.handle, args=(client,))
                t.setDaemon(self.daemon)
                t.start()
            except KeyboardInterrupt:
                raise
            except Exception as x:
                logger.exception(x)

    def handle(self, client):
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException as tx:
            pass
        except Exception as x:
            logger.exception(x)

        itrans.close()
        otrans.close()


# Queue + 10 threads(不太支持长连接)
class TThreadPoolServer(TServer):
    """Server with a fixed size pool of threads which service requests."""

    def __init__(self, *args, **kwargs):
        TServer.__init__(self, *args)
        self.clients = Queue.Queue()
        self.threads = 10
        self.daemon = kwargs.get("daemon", False)

    def setNumThreads(self, num):
        """Set the number of worker threads that should be created"""
        self.threads = num

    def serveThread(self):
        """Loop around getting clients from the shared queue and process them."""
        while True:
            try:
                client = self.clients.get()
                self.serveClient(client)
            except Exception as x:
                logger.exception(x)

    def serveClient(self, client):
        """Process input/output from a client for as long as possible"""
        itrans = self.inputTransportFactory.getTransport(client)
        otrans = self.outputTransportFactory.getTransport(client)
        iprot = self.inputProtocolFactory.getProtocol(itrans)
        oprot = self.outputProtocolFactory.getProtocol(otrans)
        try:
            while True:
                self.processor.process(iprot, oprot)
        except TTransport.TTransportException as tx:
            pass
        except Exception as x:
            logger.exception(x)

        itrans.close()
        otrans.close()

    def serve(self):
        """Start a fixed number of worker threads and put client into a queue"""
        # 创建一个线程池(10个线程)
        for i in range(self.threads):
            try:
                t = threading.Thread(target=self.serveThread)
                t.setDaemon(self.daemon)
                t.start()
            except Exception as x:
                logger.exception(x)

        # Pump the socket for clients
        # 新来的Client放入Queue, 然后各个线程分别处理一个Client, 知道处理完毕(不支持长连接，否则某些连接始终得不到处理)
        self.serverTransport.listen()
        while True:
            try:
                client = self.serverTransport.accept()
                if not client:
                    continue
                self.clients.put(client)
            except Exception as x:
                logger.exception(x)


# 和 TThreadedServer 类似，只是通过fork进程来处理新来的请求(进程复用? 但是似乎对长连接的支持不太友好)
class TForkingServer(TServer):
    """A Thrift server that forks a new process for each request

    This is more scalable than the threaded server as it does not cause
    GIL contention.

    Note that this has different semantics from the threading server.
    Specifically, updates to shared variables will no longer be shared.
    It will also not work on windows.

    This code is heavily inspired by SocketServer.ForkingMixIn in the
    Python stdlib.
    """

    def __init__(self, *args):
        TServer.__init__(self, *args)
        self.children = []

    def serve(self):
        def try_close(file):
            try:
                file.close()
            except IOError as e:
                logger.warning(e, exc_info=True)

        self.serverTransport.listen()
        while True:
            client = self.serverTransport.accept()
            if not client:
                continue
            # 来一个请求，fork一个进程
            try:
                pid = os.fork()

                if pid:  # parent
                    # add before collect, otherwise you race w/ waitpid
                    self.children.append(pid)
                    self.collect_children()

                    # Parent must close socket or the connection may not get
                    # closed promptly
                    itrans = self.inputTransportFactory.getTransport(client)
                    otrans = self.outputTransportFactory.getTransport(client)
                    try_close(itrans)
                    try_close(otrans)
                else:
                    itrans = self.inputTransportFactory.getTransport(client)
                    otrans = self.outputTransportFactory.getTransport(client)

                    iprot = self.inputProtocolFactory.getProtocol(itrans)
                    oprot = self.outputProtocolFactory.getProtocol(otrans)

                    ecode = 0
                    try:
                        try:
                            while True:
                                self.processor.process(iprot, oprot)
                        except TTransport.TTransportException as tx:
                            pass
                        except Exception as e:
                            logger.exception(e)
                            ecode = 1
                    finally:
                        try_close(itrans)
                        try_close(otrans)

                    os._exit(ecode)

            except TTransport.TTransportException as tx:
                pass
            except Exception as x:
                logger.exception(x)

    def collect_children(self):
        while self.children:
            try:
                pid, status = os.waitpid(0, os.WNOHANG)
            except os.error:
                pid = None

            if pid:
                self.children.remove(pid)
            else:
                break
