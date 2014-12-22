try:
   import gevent
   from gevent.local import local
   from zmq import green as zmq
except:
   from threading import local
   import zmq

class AggregatorNotReadyException(Exception):
    pass

import logging
log = logging.getLogger(__name__)


class _RemoteMethod:
    def __init__(self, socket, poller, name):
        self.socket = socket
        self.poller = poller
        self.name = name
        events = self.poller.poll(2.0)

        if events:
            mask = events[0][1]
            if mask & zmq.POLLERR:
                raise AggregatorNotReadyException('pollerror')

        if not events:
            raise AggregatorNotReadyException('not ready')

    def __call__(self, *args, **kwargs):
        self.socket.send_pyobj((self.name, args, kwargs))
        count = 40
        while True:
            try:
                return self.socket.recv_pyobj(zmq.DONTWAIT)
            except zmq.Again:
                count -= 1
                if count < 1:
                    _local.aggregator.reset_control_socket()
                    raise AggregatorNotReadyException('not ready for result')
                else:
                    gevent.sleep(0.025)



class Aggregator(object):
    def __init__(self):
        self.context = zmq.Context()
        self.data_socket = self.context.socket(zmq.PUB)
        self.data_socket.connect ("tcp://localhost:5556")
        self.control_socket = self.context.socket(zmq.REQ)
        self.control_socket.connect("tcp://localhost:5557")
        self.control_poller = zmq.Poller()
        self.control_poller.register(self.control_socket, zmq.POLLOUT)

    def insert(self, tags, values):
        self.insert_all([(tags, values)])
        
    def insert_all(self, items):
        self.data_socket.send_pyobj(items)

    def reset_control_socket(self):
        self.control_socket.setsockopt(zmq.LINGER, 0)
        self.control_socket.close()
        self.control_poller.unregister(self.control_socket)
        self.control_socket = self.context.socket(zmq.REQ)
        self.control_socket.connect("tcp://localhost:5557")
        self.control_poller.register(self.control_socket, zmq.POLLOUT|zmq.POLLERR|zmq.POLLIN)

    def __getattr__(self, name):
        try:
            return _RemoteMethod(self.control_socket, self.control_poller, name)
        except AggregatorNotReadyException:
            self.reset_control_socket()
            raise


    
    def ping(self):
        events = self.data_poller.poll(0.01)
        if events:
            self.data_socket.send_pyobj(None)


_local = local()

def get_client():
    try:
        return _local.aggregator
    except AttributeError:
        _local.aggregator = Aggregator()
        return _local.aggregator
